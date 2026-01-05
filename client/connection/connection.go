package connection

import (
	"slices"
	"time"

	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var Topics = []*razpravljalnica.Topic{
	{
		Id:   1,
		Name: "Topic1",
	},
	{
		Id:   2,
		Name: "TopicWithAnUnusuallyLongName",
	},
	{
		Id:   3,
		Name: "Empty",
	},
}

var Messages = []*razpravljalnica.Message{
	{
		Id:        1,
		TopicId:   1,
		UserId:    1,
		Text:      "Message written by User 1 in topic 1",
		CreatedAt: timestamppb.New(time.Now().Add(-time.Hour)),
		Likes:     5,
	},
	{
		Id:        2,
		TopicId:   1,
		UserId:    2,
		Text:      "Message written by User 2 in topic 1",
		CreatedAt: timestamppb.New(time.Now().Add(-time.Hour + time.Minute)),
		Likes:     2,
	},
	{
		Id:        3,
		TopicId:   1,
		UserId:    1,
		Text:      "Message written by User 1 in topic 1",
		CreatedAt: timestamppb.New(time.Now().Add(-time.Hour + 2*time.Minute)),
		Likes:     0,
	},
	{
		Id:        4,
		TopicId:   2,
		UserId:    1,
		Text:      "Message written by User 1 in topic 2",
		CreatedAt: timestamppb.New(time.Now().Add(-time.Hour)),
		Likes:     0,
	},
}

var Users = []*razpravljalnica.User{
	{
		Id:   1,
		Name: "User1",
	},
	{
		Id:   2,
		Name: "User2",
	},
	{
		Id:   3,
		Name: "User3WithVeryLongUserName",
	},
}

func GetTopicMessages(topicId int64) []*razpravljalnica.Message {
	return slices.Collect(func(yield func(*razpravljalnica.Message) bool) {
		for _, message := range Messages {
			if message.TopicId == topicId {
				if !yield(message) {
					return
				}
			}
		}
	})
}

func GetUserById(userId int64) *razpravljalnica.User {
	index := slices.IndexFunc(Users, func(u *razpravljalnica.User) bool {
		return u.Id == userId
	})

	if index == -1 {
		return &razpravljalnica.User{}
	}

	return Users[index]
}
