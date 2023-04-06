package main

import (
	"database/sql"
	"fmt"
)

func getOrders(db *sql.DB) ([]Order, error) {
	var orders []Order
	querySql := "SELECT O.order_uid, \n" +
		"O.track_number, \n" +
		"O.entry, \n" +
		"O.locale, \n" +
		"O.internal_signature, \n" +
		"O.customer_id, \n" +
		"O.delivery_service, \n" +
		"O.shardkey, \n" +
		"O.sm_id, \n" +
		"O.date_created, \n" +
		"O.oof_shard, \n" +
		"D.name, \n" +
		"D.phone, \n" +
		"D.zip, \n" +
		"D.city, \n" +
		"D.address, \n" +
		"D.region, \n" +
		"D.email, \n" +
		"P.transaction, \n" +
		"P.request_id, \n" +
		"P.currency, \n" +
		"P.provider, \n" +
		"P.amount, \n" +
		"P.payment_dt, \n" +
		"P.bank, \n" +
		"P.delivery_cost, \n" +
		"P.goods_total, \n" +
		"P.custom_fee \n" +
		"FROM orders AS O\n" +
		"JOIN delivery AS D\n" +
		"ON D.order_id = O.order_uid\n" +
		"JOIN payment AS P\n" +
		"ON P.order_id = O.order_uid"

	rows, err := db.Query(querySql)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		p := Order{}
		err := rows.Scan(&p.OrderUid,
			&p.TrackNumber,
			&p.Entry,
			&p.Locale,
			&p.InternalSignature,
			&p.CustomerId,
			&p.DeliveryService,
			&p.Shardkey,
			&p.SmId,
			&p.DateCreated,
			&p.OofShard,
			&p.Delivery.Name,
			&p.Delivery.Phone,
			&p.Delivery.Zip,
			&p.Delivery.City,
			&p.Delivery.Address,
			&p.Delivery.Region,
			&p.Delivery.Email,
			&p.Payment.Transaction,
			&p.Payment.RequestId,
			&p.Payment.Currency,
			&p.Payment.Provider,
			&p.Payment.Amount,
			&p.Payment.PaymentDt,
			&p.Payment.Bank,
			&p.Payment.DeliveryCost,
			&p.Payment.GoodsTotal,
			&p.Payment.CustomFee)

		if err != nil {
			fmt.Println(err)
			return orders, err
		}
		orders = append(orders, p)
	}
	//Шаг 2 - добавление в Orders Items
	for i := range orders {
		var items []Items
		querySqlItems := "SELECT chrt_id, \n" +
			"track_number, \n" +
			"price, \n" +
			"rid, \n" +
			"name, \n" +
			"sale, \n" +
			"size, \n" +
			"total_price, \n" +
			"nm_id, \n" +
			"brand, \n" +
			"status \n" +
			"FROM items \n" +
			"WHERE order_id = $1"

		rows, err = db.Query(querySqlItems, orders[i].OrderUid)

		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			item := Items{}
			err := rows.Scan(&item.ChrtId,
				&item.TrackNumber,
				&item.Rid,
				&item.Name,
				&item.Sale,
				&item.Size,
				&item.TotalPrice,
				&item.NmId,
				&item.Brand,
				&item.Status)

			if err != nil {
				fmt.Println(err)
				return orders, err
			}

			items = append(items, item)
		}
		orders[i].Items = items
	}

	return orders, nil
}

// сохранение данных в postgres
func createOrder(db *sql.DB, order Order) error {
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Ошибка начала транзакции")
		return err
	}
	defer func(tx *sql.Tx) {
		err = tx.Rollback()
	}(tx)

	query := "INSERT \n" +
		" INTO orders(\n" +
		" order_uid,\n" +
		" track_number,\n" +
		" entry,\n" +
		" locale,\n" +
		" internal_signature,\n" +
		" customer_id,\n" +
		" delivery_service,\n" +
		" shardkey,\n" +
		" sm_id,\n" +
		" date_created,\n" +
		" oof_shard ) \n" +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"

	_, err = tx.Exec(query, order.OrderUid,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.Shardkey,
		order.SmId,
		order.DateCreated,
		order.OofShard)
	if err != nil {
		return err
	}

	query = "INSERT \n" +
		" INTO delivery(\n" +
		" name,\n" +
		" phone,\n" +
		" zip,\n" +
		" city,\n" +
		" address,\n" +
		" region,\n" +
		" email,\n" +
		" order_id)\n" +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

	_, err = tx.Exec(query, order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
		order.OrderUid)

	if err != nil {
		return err
	}
	query = "INSERT \n" +
		" INTO payment(\n" +
		" transaction,\n" +
		" request_id,\n" +
		" currency,\n" +
		" provider,\n" +
		" amount,\n" +
		" payment_dt,\n" +
		" bank,\n" +
		" delivery_cost,\n" +
		" goods_total,\n" +
		" custom_fee,\n" +
		" order_id)\n" +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"

	_, err = tx.Exec(query, order.Payment.Transaction,
		order.Payment.RequestId,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
		order.OrderUid)

	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, item := range order.Items {
		query = "INSERT \n" +
			" INTO items(\n" +
			" chrt_id,\n" +
			" track_number,\n" +
			" price,\n" +
			" rid,\n" +
			" name,\n" +
			" sale,\n" +
			" size,\n" +
			" total_price,\n" +
			" nm_id,\n" +
			" brand,\n" +
			" status,\n" +
			" order_id)\n" +
			"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"

		_, err = tx.Exec(query, item.ChrtId,
			item.TrackNumber,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.Size,
			item.TotalPrice,
			item.NmId,
			item.Brand,
			item.Status,
			order.OrderUid)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("Ошибка закрытия транзакции")
		return err
	}
	return nil
}
