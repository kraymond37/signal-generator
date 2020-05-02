package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"log"
	"time"
)

var server *machinery.Server

func main() {
	if server = startUpService(); server == nil {
		return
	}
	fmt.Println("start service")

	//if err := startTradeFollower(); err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Println("start follower")

	//if err := stopTradeFollower(); err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Println("start follower")

	//if err := startTradeMonitor(); err != nil {
	//	fmt.Println(err)
	//	_ = stopTradeFollower()
	//	return
	//}
	//fmt.Println("start monitor")

	//sendTradeSignalToFollower()
}

func startUpService() *machinery.Server {
	cfg, err := config.NewFromYaml("robot.yaml", true)
	if err != nil {
		fmt.Println(cfg, err)
		return nil
	}

	server, err := machinery.NewServer(cfg)
	if err != nil {
		fmt.Println(cfg, err)
		return nil
	}

	return server
}

func startTradeMonitor() error {
	// bm3
	targetInfo := map[string]interface{}{
		"target_account": "bm3",
		"exchange":       "bitmex",
		"base_host":      "testnet.bitmex.com",
		"ws_host":        "testnet.bitmex.com",
		"access_key":     "jAwOyiesXELlb-lHi3j3zhkp",
		"secret_key":     "ydYMBfO0dYcFwDonwGZuwyJ3debrLJMonkL23BNLFZC8mxJZ",
		"symbols":        []interface{}{"XBTUSD"},
		"ding": map[string]interface{}{
			"ding":   "",
			"phones": "",
		},
	}

	data, err := json.Marshal(targetInfo)
	if err != nil {
		return err
	}

	var startTradeMonitor = tasks.Signature{
		Name: "startTradeMonitor",
		Args: []tasks.Arg{
			{
				Type:  "[]byte",
				Value: data,
			},
		},
		RoutingKey: "quantized-sponsor",
	}

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := "Q" + uuid.New().String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))

	return sendTask(ctx, startTradeMonitor)
}

func startTradeFollower() error {
	followInfos := []map[string]interface{}{getBm4FollowerInfo(), getBm5FollowerInfo()}

	data, err := json.Marshal(followInfos)
	if err != nil {
		return err
	}

	var startFollower = tasks.Signature{
		Name: "startTradeFollower",
		Args: []tasks.Arg{
			{
				Type:  "[]byte",
				Value: data,
			},
		},
		RoutingKey: "quantized-follower",
	}
	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := uuid.New().String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))
	return sendTask(ctx, startFollower)
}

func stopTradeFollower() error {
	followInfos := []map[string]interface{}{getBm4FollowerInfo(), getBm5FollowerInfo()}
	data, err := json.Marshal(followInfos)
	if err != nil {
		return err
	}

	var startFollower = tasks.Signature{
		Name: "stopTradeFollower",
		Args: []tasks.Arg{
			{
				Type:  "[]byte",
				Value: data,
			},
		},
		RoutingKey: "quantized-follower",
	}
	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := uuid.New().String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))
	return sendTask(ctx, startFollower)
}

func sendTradeSignalToFollower() {
	params := map[string]interface{}{
		"symbol":              "ETHUSD",
		"side":                "Buy",
		"amount":              int64(1),
		"timestamp":           time.Now(),
		"target_account":      "bm3",
		"exchange":            "bitmex",
		"order_id":            "6868686868",
		"order_status":        "Filled",
		"order_amount":        int64(1),
		"order_filled_amount": int64(1),
		"order_time":          time.Now(),
		"order_avg_price":     123.456,
	}
	signal, _ := json.Marshal(params)

	var task = tasks.Signature{
		Name: "broadcastTradeSignal",
		Args: []tasks.Arg{
			{
				Type:  "[]byte",
				Value: signal,
			},
		},
		RoutingKey: "quantized-follower",
	}
	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := "Q" + uuid.New().String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))
	_ = sendTask(ctx, task)
}

func getBm5FollowerInfo() map[string]interface{} {
	followerInfo := map[string]interface{}{
		"target_account": "bm3",
		"follow_account": "bm5",
		"exchange":       "bitmex",
		"contracts": []map[string]interface{}{
			{
				"id":             "105",
				"task_title":     "test-bm5",
				"contract":       "XBTUSD",
				"buy_limit_max":  "1000",
				"buy_limit_min":  "0",
				"sell_limit_max": "1000",
				"sell_limit_min": "0",
				"price_offset":   "2",
				"cancel_time":    "5",
				"follow_rate":    "1.0",
			},
		},
		"base_host":         "testnet.bitmex.com",
		"ws_host":           "testnet.bitmex.com",
		"access_key":        "9U870JJgfMDFo2Vip6xqlIUg",
		"secret_key":        "9sTMUVJv2KeLkA1FkYHa3q8t7K3Sfn_FgM6iHCCT-NCIYLcN",
		"target_access_key": "jAwOyiesXELlb-lHi3j3zhkp",
		"target_secret_key": "ydYMBfO0dYcFwDonwGZuwyJ3debrLJMonkL23BNLFZC8mxJZ",
		"ding": map[string]interface{}{
			"ding":   "https://oapi.dingtalk.com/robot/send?access_token=2b23786b0d7661a04c971a2fcde9884c7423fef50c6768e579ba283c7f5d5f1d",
			"phones": []string{"15988472906"},
		},
	}

	return followerInfo
}

func getBm4FollowerInfo() map[string]interface{} {
	followerInfo := map[string]interface{}{
		"target_account": "bm3",
		"follow_account": "bm4",
		"exchange":       "bitmex",
		"contracts": []map[string]interface{}{
			{
				"id":             "104",
				"task_title":     "test-bm4",
				"contract":       "XBTUSD",
				"buy_limit_max":  "1000",
				"buy_limit_min":  "0",
				"sell_limit_max": "1000",
				"sell_limit_min": "0",
				"price_offset":   "2",
				"cancel_time":    "5",
				"follow_rate":    "1.0",
			},
		},
		"base_host":         "testnet.bitmex.com",
		"ws_host":           "testnet.bitmex.com",
		"access_key":        "F8vOWWaSt2olHd8rUNGnt1gQ",
		"secret_key":        "15buQgLJDoCaSmXLA9TxZey-9f3S1ybqbbeIj09VGDcBkm55",
		"target_access_key": "jAwOyiesXELlb-lHi3j3zhkp",
		"target_secret_key": "ydYMBfO0dYcFwDonwGZuwyJ3debrLJMonkL23BNLFZC8mxJZ",
		"ding": map[string]interface{}{
			"ding":   "https://oapi.dingtalk.com/robot/send?access_token=2b23786b0d7661a04c971a2fcde9884c7423fef50c6768e579ba283c7f5d5f1d",
			"phones": []string{"15988472906"},
		},
	}

	return followerInfo
}

func sendTask(ctx context.Context, signature tasks.Signature) error {
	asyncResult, err := server.SendTaskWithContext(ctx, &signature)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	fmt.Println("send task")

	results, err := asyncResult.Get(time.Millisecond * 5)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err.Error())
	}
	log.Println(tasks.HumanReadableResults(results))
	return nil
}
