package column_test

// func TestColumnReadError(t *testing.T) {
// 	startValidReader := 15

// 	tests := []struct {
// 		name        string
// 		wantErr     string
// 		numberValid int
// 	}{
// 		{
// 			name:        "blockInfo: read field1",
// 			wantErr:     "block: temporary table",
// 			numberValid: startValidReader - 1,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
// 			require.NoError(t, err)
// 			config.ReaderFunc = func(r io.Reader) io.Reader {
// 				return &readErrorHelper{
// 					err:         errors.New("timeout"),
// 					r:           r,
// 					numberValid: tt.numberValid,
// 				}
// 			}

// 			c, err := chconn.ConnectConfig(context.Background(), config)
// 			assert.NoError(t, err)
// 			stmt, err := c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 1;")
// 			require.NoError(t, err)
// 			require.False(t, stmt.Next())

// 			require.Error(t, stmt.Err())
// 			require.EqualError(t, errors.Unwrap(stmt.Err()), "timeout")
// 			for stmt.Next() {

// 			}
// 		})
// 	}
// }
