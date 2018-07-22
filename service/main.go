package main

import (
	"fmt"
	"encoding/json"
	"net/http"
	"log"
	"strconv"
	elastic "gopkg.in/olivere/elastic.v3"
	"reflect"
	"strings"
	"github.com/pborman/uuid"
	"context"
	"cloud.google.com/go/storage"
	"io"
	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"

)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}


type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
	Url string `json:"url"`
}


const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	ES_URL = "http://35.226.133.227:9200"
	BUCKET_NAME = "around-prod"
)

var mySigningKey = []byte("secret")


func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{																						"mappings":{																						"post":{
					"properties":{																						"location":{																						"type":"geo_point"
					          }
					}
				     }																					}
		}`
	_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service")

	r := mux.NewRouter()
	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter : func(token *jwt.Token)(interface{}, error) { // jwt get token. we can also perform transformations on the keys in the later
			return mySigningKey, nil				// e.g. different keys for different dates
		},
		SigningMethod: jwt.SigningMethodHS256,
	})



	//http.HandleFunc("/post", handlerPost)
	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	//http.HandleFunc("/search", handlerSearch)
	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

// {
//	"user" : "john",
//	"message" : "Test",
//	"location" : {
//		"lat" : 37,
//		"lon" : -120
//	}
// }


func handlerPost(w http.ResponseWriter, r *http.Request) { // in Go we pass by value, so we can use pointer to avoid deep copy
	w.Header().Set("Content-type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Header", "Content-Type,Authorization")

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	r.ParseMultipartForm(32 << 20)

	//Parse form data
	fmt.Printf("Received one post request%s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User : username.(string),
		Message: r.FormValue("message"),
		Location: Location {
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	file,_,err := r.FormFile("image")
	// jubing, fileHeader, err
	if err != nil {
		http.Error(w, "image is not setup", http.StatusInternalServerError)
		fmt.Printf("image is not setup %v\n", err) // we can use %v to output any type of data
		panic(err)
	}

	defer file.Close()

	ctx := context.Background()

	_, attrs, err := saveToGCS(ctx,file, BUCKET_NAME, id)

	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}

	p.Url = attrs.MediaLink

	//Save to ES
	saveToES(p, id) // p is now a pointer so take the % off
}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// as long as we implement io.Reader interface, we can use io.Copy
	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, nil, err
	}

	defer client.Close()

	bucket := client.Bucket(BUCKET_NAME)

	// Next check if the bucket exists
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)
	wc := obj.NewWriter(ctx) // writer client

	if _, err := io.Copy(wc, r); err != nil {
		return nil, nil, err
	}

	if err := wc.Close(); err != nil {
		return nil, nil, err
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)

	return obj, attrs, err
}

func saveToES(p *Post, id string) {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL),
			elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	_, err = es_client.Index().Index(INDEX).Type(TYPE).Id(id).BodyJson(p).Refresh(true).Do()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Post with id %s is saved to index: %s\n", id, p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search.")

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	ran := DISTANCE

	if val:= r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received. %f % f %s\n", lat, lon, ran)

	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // sniff : elastic's library, there is a callback to do logging.
	if err != nil {
		panic(err)
	}

	q := elastic.NewGeoDistanceQuery("location") // query name
	q = q.Distance(ran).Lat(lat).Lon(lon) // return itself each time when you call

	searchResult, err := client.Search().
			Index(INDEX).
			Query(q).
			Pretty(true).
			Do()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d posts \n", searchResult.TotalHits())

	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // equivalent to Java instanceOf
		p := item.(Post) // p = (Post) item //casting
		if shouldFilter(p.Message) {
			ps = append(ps, p) // we put all the search results in the slide ps

		}
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)

}

func shouldFilter(post string) bool { // TODO: change to hash set
	filtered_words :=[]string {"fuck", "shit"}
	for _, word := range filtered_words {
		if strings.Contains(post, word) {
			return false
		}
	}
	return true
}
