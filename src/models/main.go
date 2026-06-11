package apiModels

type OpenaiModelsList struct {
	Object string        `json:"object"`
	Data   []OpenaiModel `json:"data"`
}

type OpenaiModel struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created,omitempty"`
	Owned_by string `json:"owned_by,omitempty"`
}

type MazeModel struct {
	Id           string   `json:"id"`
	Aliases      []string `json:"aliases,omitempty"`
	ProviderName string   `json:"provider"`
}

type MazeModelsList struct {
	Models []MazeModel `json:"models"`
}
