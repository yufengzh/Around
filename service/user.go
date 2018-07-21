package main

import (
      elastic "gopkg.in/olivere/elastic.v3"

      "encoding/json"
      "fmt"
      "net/http"
      "reflect"
      "regexp"
      "time"

      "github.com/dgrijalva/jwt-go"
)

const (
      TYPE_USER = "user"
)

var (
      usernamePattern = regexp.MustCompile(`^[a-z0-9_]+$`).MatchString
      /*
	^ - from the start of the string
	$ - to the end of the string
	[] - one character - from a - z, 0 - 9
	_ - range
	+ - at least one (one or more match)
        * - zero or more
	*/
)

type User struct {
      Username string `json:"username"`
      Password string `json:"password"`
      Age int `json:"age"`
      Gender string `json:"gender"`
}

// checkUser check whether user is valid

func checkUser(username, password string) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not set up %v\n", err)
		return false
	}

	// Search with a term query (keyword)
	termQuery := elastic.NewTermQuery("username", username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()

	if err != nil {
		fmt.Printf("ES query failed %v \n", err)
		return false
	}

	var tyu User
	for _, item := range queryResult.Each(reflect.TypeOf(tyu)) { // if we use search, the return type is always a slice. technically, there is only one record in the loop
		u := item.(User)
		return u.Password == password && u.Username == username
	}

	return false
}

// Add a new user Return true if successful
func addUser(user User) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
	}

	termQuery := elastic.NewTermQuery("username", user.Username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()

	if err != nil {
		fmt.Printf("ES Query failed %v\n", err)
		panic(err)
	}

	if queryResult.TotalHits() > 0 {
		fmt.Printf("User %s already exists, cannot create duplicate users.\n", user.Username)
		return false
	}

	_, err = es_client.Index(). // cannot use := here because we've already defined err
		Index(INDEX).
		Type(TYPE_USER).
		Id(user.Username).
		BodyJson(user). // BodyJson, instead of Body
		Refresh(true). // if duplicate then refresh
		Do()

	if err != nil {
		fmt.Printf("ES save user failed. %v \n", err)
		return false
	}

	return true
}


// If signup is successful, a new session is created.
func signupHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received an signup request")
	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		http.Error(w, "sign up json string is illegal", 406)
		return
	}
	if u.Username != "" && u.Password != "" && usernamePattern(u.Username) {
		if addUser(u) {
			fmt.Println("User added successfully.")
			w.Write([]byte("User added successfully."))
		} else {
			fmt.Println("Failed to add a new user.")
			http.Error(w, "Failed to add a new user", http.StatusInternalServerError)
		}
	} else {
		fmt.Println("Username and/or password is illegal")
		http.Error(w, "Username and/or password is invalid", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// If login is successful, a new token is created.
func loginHandler(w http.ResponseWriter, r *http.Request) {
      fmt.Println("Received one login request")

      decoder := json.NewDecoder(r.Body)
      var u User
      if err := decoder.Decode(&u); err != nil {
             panic(err)
             return
      }

      if checkUser(u.Username, u.Password) {
             token := jwt.New(jwt.SigningMethodHS256)
             claims := token.Claims.(jwt.MapClaims)
             /* Set token claims */
             claims["username"] = u.Username
             claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

             /* Sign the token with our secret */
             tokenString, _ := token.SignedString(mySigningKey)

             /* Finally, write the token to the browser window */
             w.Write([]byte(tokenString))
      } else {
             fmt.Println("Invalid password or username.")
             http.Error(w, "Invalid password or username", http.StatusForbidden)
      }

      w.Header().Set("Content-Type", "text/plain")
      w.Header().Set("Access-Control-Allow-Origin", "*")
}
