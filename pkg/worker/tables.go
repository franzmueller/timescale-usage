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
	"fmt"
)

func (w *Worker) getTables() (tables []string, err error) {
	rows, err := w.conn.Query(fmt.Sprintf("SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema = '%v';", w.config.PostgresSourceSchema))
	if err != nil {
		return nil, err
	}
	tables = []string{}
	var table string
	for rows.Next() {
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

type viewInfo struct {
	view       string
	hypertable string
}

func (w *Worker) getViews() (views []viewInfo, err error) {
	rows, err := w.conn.Query(fmt.Sprintf("SELECT view_name, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = '%v';", w.config.PostgresSourceSchema))
	if err != nil {
		return nil, err
	}
	views = []viewInfo{}
	var view viewInfo
	for rows.Next() {
		err = rows.Scan(&view.view, &view.hypertable)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}
