package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// w запимваем , r читаем
func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	// проверяем метод запроса
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusBadRequest)
		return
	}

	// парсим JSON-данные из тела запроса
	var requestData struct {
		ID string `json:"id"`
	}
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Некорректный JSON", http.StatusBadRequest)
		return
	}

	fmt.Println(requestData)
	var response, ok = cache[requestData.ID]
	if !ok {
		http.Error(w, "orderNotFound", http.StatusNotFound)
		return
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Ошибка сериализации JSON", http.StatusInternalServerError)
		return
	}

	// отправляем ответ клиенту
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if code, err := w.Write(responseJSON); err != nil {
		fmt.Println(code)
		return
	}
	return
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusBadRequest)
		return
	}

	var order Order
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "statusMessageInternalServerError", http.StatusInternalServerError)
		return
	}

	err = r.Body.Close()
	if err != nil {
		http.Error(w, "statusMessageInternalServerError", http.StatusInternalServerError)
		return
	}

	err = json.Unmarshal(requestBody, &order)
	if err != nil {
		http.Error(w, "statusUnsupportedMediaType", http.StatusUnsupportedMediaType)
		return
	}

	err = order.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Publish the message to NATS streaming server
	err = sc.Publish("test-channel", requestBody)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
	fmt.Println("Message published successfully.")

	w.WriteHeader(http.StatusNoContent)
	return
}
