package schema

// Subscription messages
type SubscribeBinanceTicker struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int      `json:"id"`
}
type SubscribeBybitTicker struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}
type SubscribeKucoinTicker struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
}
type SubscribeMexcTicker struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type OKXArg struct {
	Channel string `json:"channel"`
	InstId  string `json:"instId"`
}
type SubscribeOKXTicker struct {
	Op   string `json:"op"`
	Args []OKXArg `json:"args"`
}
