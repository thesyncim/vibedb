package shardservice

import (
	"bytes"
	"testing"
)

func BenchmarkDecodeRowDelivery(b *testing.B) {
	columns := []Column{{Name: "id", TypeOID: pgOIDJSON}, {Name: "payload", TypeOID: pgOIDJSON}}
	rows := make([][]Cell, 4096)
	for i := range rows {
		rows[i] = []Cell{
			{Bytes: []byte(`"0123456789abcdef"`)},
			{Bytes: []byte(`{"active":true,"score":42}`)},
		}
	}
	cases := []struct {
		name string
		resp *ShardResponse
	}{
		{name: "single_frame", resp: RowsResponse(columns, rows)},
		{name: "row_batch", resp: &ShardResponse{
			Kind: ResponseRowBatch, Columns: columns, Rows: rows,
			RowBatch: RowBatchReply{ColumnCount: 2, Final: true},
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var frame bytes.Buffer
			if err := EncodeResponse(&frame, tc.resp); err != nil {
				b.Fatal(err)
			}
			raw := frame.Bytes()
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeResponse(bytes.NewReader(raw)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
