package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/rs/cors"
	"net/http"
	"time"
)

var cache = make(map[string]Order)
var sc stan.Conn

func main() {
	//подключаемся к БД
	var db, err = sql.Open("postgres",
		fmt.Sprintf("host=%s port=%d sslmode=%s dbname=%s user=%s password=%s",
			"localhost", 54321, "disable", "postgres", "postgres", "postgres1234"))
	if err != nil {
		return
	}
	//получаем все заказы из бд
	orders, err := getOrders(db)
	if err != nil {
		fmt.Println("Ошибка чтения Orders")
		return
	}
	//заказы из бд сохраняем в кэш
	for _, order := range orders {
		cache[order.OrderUid] = order
	}

	// Подключаемся к NATS Streaming серверу
	sc, err = stan.Connect("test-cluster", "client-1", stan.NatsURL(nats.DefaultURL))
	if err != nil {
		fmt.Println("Error connecting to NATS Streaming:", err)
		return
	}
	defer sc.Close()

	// Подписываемся на канал "test-channel" и обрабатываем сообщения
	sub, err := sc.Subscribe("test-channel", func(msg *stan.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
		//Получаем message data, делаем unmarshall с помощью структуры orders
		// ошибка "получили неверные данные", расшифрованные данные сохраняем в cache

		var order Order
		if err := json.Unmarshal(msg.Data, &order); err != nil {
			fmt.Println(err)
			return
		}
		err := order.Validate()
		if err != nil {
			fmt.Println(err)
			return
		}

		err = createOrder(db, order) //сохранение в postgres
		if err != nil {
			fmt.Println(err)
			return
		}
		cache[order.OrderUid] = order //сохранение в кэше

	}, stan.DurableName("my-durable-name"))

	if err != nil {
		fmt.Println("Error subscribing to channel:", err)
		return
	}
	defer sub.Unsubscribe()
	//создание веб сервера
	router := mux.NewRouter()
	//корс - доп информацию, политика безопастности, здесь разрешаем все запросы
	router.Use(cors.AllowAll().Handler)
	router.HandleFunc("/GetOrder", getOrderHandler)
	router.HandleFunc("/CreateOrder", createOrderHandler)

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "assets/html/index.html")
	})

	router.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "assets/html/send.html")
	})

	server := &http.Server{
		Addr:           ":8080",
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
	// Бесконечно ждем сообщений
	select {}
}
