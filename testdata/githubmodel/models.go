package githubmodel

import (
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/testdata/githubmodelout"
)

//chtuplegen:json
type Releases struct {
	Version string
}

type Repository struct {
	URL      string `json:"url"`
	Releases []Releases
}

type Achievement struct {
	Name        string
	AwardedDate time.Time
}

type Account struct {
	Id             uint32
	Name           string
	Organizations  []string `json:"orgs"`
	Repositories   []Repository
	Achievement    Achievement
	AchievementOut githubmodelout.AchievementOut
}

type GithubEvent struct {
	Title        string
	Type         string
	Assignee     Account  `json:"assignee"`
	Labels       []string `json:"labels"`
	Contributors []Account
}
