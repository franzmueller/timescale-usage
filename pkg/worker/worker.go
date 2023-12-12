/*
 *    Copyright 2023 InfAI (CC SES)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/SENERGY-Platform/timescale-usage/pkg/configuration"
)

type Worker struct {
	conn   *pgx.Conn
	config configuration.Config
}

func Start(ctx context.Context, config configuration.Config) error {
	conn, err := pgx.Connect(pgx.ConnConfig{
		Host:     config.PostgresHost,
		Port:     config.PostgresPort,
		Database: config.PostgresDb,
		User:     config.PostgresUser,
		Password: config.PostgresPw,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	w := &Worker{conn: conn, config: config}
	return w.run()

}

func (w *Worker) run() (err error) {
	err = w.migrate()
	if err != nil {
		return err
	}

	//err = w.upsertTables()
	if err != nil {
		return err
	}

	err = w.upsertViews()
	if err != nil {
		return err
	}

	// Cleanup outdated
	log.Println("Cleanup")
	_, err = w.conn.Exec(fmt.Sprintf("DELETE FROM %v.usage where \"table\" NOT IN (SELECT hypertable_name FROM timescaledb_information.hypertables  WHERE hypertable_schema = '%v') AND \"table\" NOT IN (SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = '%v');", w.config.PostgresUsageSchema, w.config.PostgresSourceSchema, w.config.PostgresSourceSchema))
	if err != nil {
		return err
	}

	log.Println("Done")
	return nil
}

func (w *Worker) upsertTables() error {
	tables, err := w.getTables()
	if err != nil {
		return err
	}
	log.Printf("Got %v tables\n", len(tables))

	for _, table := range tables {
		row := w.conn.QueryRow("SELECT hypertable_size('\"" + table + "\"');")
		var val pgtype.Int8
		err = row.Scan(&val)
		if err != nil {
			return err
		}
		var tableSizeBytes int64 = 0
		if val.Get() != nil {
			tableSizeBytes = val.Get().(int64)
		}
		log.Printf("%v %v\n", table, tableSizeBytes)

		now := time.Now().Format(time.RFC3339)
		query := fmt.Sprintf("INSERT INTO %v.usage (\"table\", bytes, updated_at) VALUES ('%v', %v, '%v') ON CONFLICT (\"table\") DO UPDATE SET bytes = %v, updated_at = '%v';", w.config.PostgresUsageSchema, table, tableSizeBytes, now, tableSizeBytes, now)
		_, err = w.conn.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) upsertViews() error {
	views, err := w.getViews()
	if err != nil {
		return err
	}
	log.Printf("Got %v views\n", len(views))

	for _, view := range views {
		row := w.conn.QueryRow("SELECT hypertable_size('_timescaledb_internal.\"" + view.hypertable + "\"');")
		var val pgtype.Int8
		err = row.Scan(&val)
		if err != nil {
			return err
		}
		var tableSizeBytes int64 = 0
		if val.Get() != nil {
			tableSizeBytes = val.Get().(int64)
		}
		log.Printf("%v %v\n", view, tableSizeBytes)

		now := time.Now().Format(time.RFC3339)
		_, err = w.conn.Exec(fmt.Sprintf("INSERT INTO %v.usage (\"table\", bytes, updated_at) VALUES ('%v', %v, '%v') ON CONFLICT (\"table\") DO UPDATE SET bytes = %v, updated_at = '%v';", w.config.PostgresUsageSchema, view.view, tableSizeBytes, now, tableSizeBytes, now))
		if err != nil {
			return err
		}
	}
	return nil
}
