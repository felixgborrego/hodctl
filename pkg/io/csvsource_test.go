package io

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCSV_ValidData(t *testing.T) {
	csvData := `"app","ts","event","project_id","source","ident","user_id","session_id","country","device_type","device_os","device_os_ver","device_browser","device_browser_ver","props","nums"
"seq-market","2024-04-15 02:15:07.167","BUY_ITEMS","4974","","1","0896ae95dcaeee38e83fa5c43bef99780d7b2be23bcab36214","5d8afd8fec2fbf3e","DE","desktop","linux","x86_64","chrome","122.0.0.0","{""currencySymbol"":""SFL""}","{""currencyValueDecimal"":""0.6136203411678249""}"
"seq-market","2024-04-15 02:26:37.134","BUY_ITEMS","0","","1","0896ae95dcaeee38e83fa5c43bef99780d7b2be23bcab36214","5d8afd8fec2fbf3e","DE","desktop","linux","x86_64","chrome","122.0.0.0","{""currencySymbol"":""SFL""}","{""currencyValueDecimal"":""0.6136203411678249""}"
`
	println("test")
	reader := strings.NewReader(csvData)
	ch, err := ReadCSV(reader, 10)
	println("test")
	assert.NoError(t, err)

	var transactions []RawTransaction
	for microBatch := range ch {
		if microBatch.Err != nil {
			assert.Fail(t, "unexpected error: %v", microBatch.Err)
		}

		transactions = append(transactions, microBatch.Data...)
	}

	expected := []RawTransaction{
		{Timestamp: "2024-04-15 02:15:07.167", ProjectID: "4974", Event: "BUY_ITEMS", Props: `{"currencySymbol":"SFL"}`, Nums: `{"currencyValueDecimal":"0.6136203411678249"}`},
		{Timestamp: "2024-04-15 02:26:37.134", ProjectID: "0", Event: "BUY_ITEMS", Props: `{"currencySymbol":"SFL"}`, Nums: `{"currencyValueDecimal":"0.6136203411678249"}`},
	}

	assert.Equal(t, expected, transactions)
}
