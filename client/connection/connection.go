package connection

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	razpravljalnica "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Connection struct {
	url               string
	client            *grpc.ClientConn
	grpcClient        razpravljalnica.MessageBoardClient
	displayUpdateFunc func()

	status     string
	statusLock sync.RWMutex

	topics       []*razpravljalnica.Topic
	topicsLock   sync.RWMutex
	messages     []*razpravljalnica.Message
	messagesLock sync.RWMutex
	users        []*razpravljalnica.User
	usersLock    sync.RWMutex
}

func NewConnection(url string) *Connection {
	conn := &Connection{
		url: url,
	}
	err := conn.Connect()
	if err != nil {
		conn.Disconnect()
	}
	return conn
}

func (c *Connection) SetDisplayUpdateFunc(f func()) {
	c.displayUpdateFunc = f
}

func (c *Connection) Connect() error {
	client, err := grpc.NewClient(c.url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	grpcClient := razpravljalnica.NewMessageBoardClient(client)

	c.client = client
	c.grpcClient = grpcClient
	return nil
}

func (c *Connection) Disconnect() {
	c.client.Close()
	c.client = nil
	c.grpcClient = nil
}

func (c *Connection) IsConnected() bool {
	if c.client == nil {
		return false
	}
	if c.grpcClient == nil {
		return false
	}
	return true
}

func (c *Connection) Status() string {
	if !c.IsConnected() {
		return "Disconnected"
	}

	c.statusLock.RLock()
	defer c.statusLock.RUnlock()
	if c.status != "" {
		return c.status
	}

	return "OK"
}

func (c *Connection) handleError(err error) {
	c.statusLock.Lock()
	c.status = fmt.Sprintf("Error: %s", err)
	c.statusLock.Unlock()

	c.displayUpdateFunc()
}

func (c *Connection) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second)
}

func (c *Connection) LoadDebugData() {
	c.topicsLock.Lock()
	c.topics = []*razpravljalnica.Topic{
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
	c.topicsLock.Unlock()

	c.messagesLock.Lock()
	c.messages = []*razpravljalnica.Message{
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
	c.messagesLock.Unlock()

	c.usersLock.Lock()
	c.users = []*razpravljalnica.User{
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
	c.usersLock.Unlock()
}

func (c *Connection) GetTopics() []*razpravljalnica.Topic {
	c.topicsLock.RLock()
	defer c.topicsLock.RUnlock()
	return c.topics
}

func (c *Connection) GetMessages() []*razpravljalnica.Message {
	c.messagesLock.RLock()
	defer c.messagesLock.RUnlock()
	return c.messages
}

func (c *Connection) GetUserById(userId int64) *razpravljalnica.User {
	c.usersLock.RLock()
	defer c.usersLock.RUnlock()
	index := slices.IndexFunc(c.users, func(u *razpravljalnica.User) bool {
		return u.Id == userId
	})

	if index == -1 {
		return &razpravljalnica.User{}
	}

	return c.users[index]
}

func (c *Connection) FetchTopics() {
	if !c.IsConnected() {
		return
	}
	ctx, cancel := c.context()
	defer cancel()

	topics, err := c.grpcClient.ListTopics(ctx, nil)
	if err != nil {
		c.handleError(err)
		return
	}

	c.topicsLock.Lock()
	defer c.topicsLock.Unlock()
	c.topics = topics.GetTopics()
}

func (c *Connection) FetchTopicMessages(topicID int64) {
	if !c.IsConnected() {
		return
	}
	ctx, cancel := c.context()
	defer cancel()

	messages, err := c.grpcClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
		TopicId: topicID,
	})
	if err != nil {
		c.handleError(err)
		return
	}

	c.messagesLock.Lock()
	defer c.messagesLock.Unlock()
	c.messages = messages.GetMessages()
}

func (c *Connection) CreateUser(username string) *razpravljalnica.User {
	if !c.IsConnected() {
		return &razpravljalnica.User{}
	}
	ctx, cancel := c.context()
	defer cancel()

	user, err := c.grpcClient.CreateUser(ctx, &razpravljalnica.CreateUserRequest{
		Name: username,
	})
	if err != nil {
		c.handleError(err)
		return &razpravljalnica.User{}
	}

	return user
}

func (c *Connection) SendMessage(req *razpravljalnica.PostMessageRequest) *razpravljalnica.Message {
	if !c.IsConnected() {
		return &razpravljalnica.Message{}
	}
	ctx, cancel := c.context()
	defer cancel()

	msg, err := c.grpcClient.PostMessage(ctx, req)
	if err != nil {
		c.handleError(err)
		return &razpravljalnica.Message{}
	}

	return msg
}
