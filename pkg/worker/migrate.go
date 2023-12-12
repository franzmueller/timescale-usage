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

func (w *Worker) migrate() error {
	_, err := w.conn.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v;", w.config.PostgresUsageSchema))
	if err != nil {
		return err
	}

	_, err = w.conn.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.usage (\"table\" varchar(63) PRIMARY KEY, bytes bigserial, updated_at timestamptz);", w.config.PostgresUsageSchema))
	if err != nil {
		return err
	}

	return nil
}
