package MongoHandles

import (
	"context"
	"log"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Conn ...class object of mongo connection
type Conn struct {
	Client *mongo.Client
}
type MongoField struct {
	Key   string
	Value string
}
type LogLine struct {
	Timestamp time.Time
	Level     string
	Message   string
}

func NewConn(URI string) (*Conn, error) {
	clientOptions := options.Client().ApplyURI(URI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}
	return &Conn{client}, nil
}

// InsertPost ...(dbName, collection, row) into the DB
func (x Conn) InsertPost(db string, coll string, row interface{}, ctx context.Context) {
	client := x.Client
	bsonPost := bson.D{}
	ref := reflect.ValueOf(row)
	for i := 0; i < ref.NumField(); i++ {
		bsonPost = append(bsonPost, bson.E{Key: ref.Type().Field(i).Name, Value: ref.Field(i).String()})
	}
	collection := client.Database(db).Collection(coll)
	log.Println("posting into database: " + db + " collection: " + coll)
	insertResult, err := collection.InsertOne(ctx, bsonPost)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("insertion complete!", insertResult.InsertedID)
}

func (x Conn) GetCollection(db string, coll string, ctx context.Context) ([]interface{}, error) {
	client := x.Client
	collection := client.Database(db).Collection(coll)
	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var count int
	var lines []interface{}
	for cursor.Next(ctx) {
		count = count + 1
		var res interface{}
		err := cursor.Decode(&res)
		lines = append(lines, res)
		if err != nil {
			return nil, err
		}
	}
	return lines, nil
}
