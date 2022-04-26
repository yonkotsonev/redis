// This is a Redis client for go that supports the usual redis commands and follows the redis-cli sintax
package redis

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	conn         net.Conn
	reader       *bufio.Reader
	host         string
	port         int
	pipeline     bool
	pipelineSize int
}

func NewClient(host string, port int) *Client {
	return &Client{conn: nil, reader: nil, host: host, port: port, pipeline: false, pipelineSize: 0}
}

func (c *Client) connect() error {
	if c.pipeline == false && (c.conn == nil || c.Ping() == false) {
		c.reset()

		conn, err := net.Dial("tcp", c.host+":"+fmt.Sprint(c.port))
		if err != nil {
			return err
		}

		c.conn = conn
		c.reader = bufio.NewReader(c.conn)
	}

	return nil
}

func (c *Client) reset() {
	if c.conn != nil {
		c.conn.Close()
	}

	c.conn = nil
	c.reader = nil
	c.pipelineSize = 0
	c.pipeline = false
}

func (c *Client) constructProtocol(args ...string) string {
	proto := ""
	proto += "*" + fmt.Sprint(len(args)) + "\r\n"
	for _, arg := range args {
		proto += "$" + fmt.Sprint(len(arg)) + "\r\n"
		proto += arg + "\r\n"
	}

	return proto
}

func (c *Client) call(args ...string) (interface{}, error) {
	if c.pipeline {
		c.pipelineSize += 1
	}

	err := c.connect()
	if err != nil {
		c.reset()

		return nil, err
	}

	command := c.constructProtocol(args...)

	_, err = c.conn.Write([]byte(command))
	if err != nil {
		c.reset()

		return nil, err
	}

	if !c.pipeline {
		return c.responseRead()
	}

	return nil, nil
}

func (c *Client) Pipeline() {
	c.connect()
	c.pipeline = true
}

func (c *Client) Execute() ([]interface{}, error) {
	resp := make([]interface{}, c.pipelineSize)
	for i := 0; i < c.pipelineSize; i++ {
		next, err := c.responseRead()
		if err != nil {
			c.reset()
			return nil, err
		}

		resp[i] = next
	}

	c.pipeline = false
	c.pipelineSize = 0

	return resp, nil
}

func (c *Client) Ping() bool {
	_, err := c.conn.Write([]byte(c.constructProtocol("ping")))
	if err != nil {
		return false
	} else {
		_, err = c.responseRead()
		if err != nil {
			return false
		}
	}

	return true
}

func (c *Client) Blpop(key string, secs int) (interface{}, error) {
	return c.call("blpop", key, fmt.Sprint(secs))
}

func (c *Client) Rpush(key string, element string) (interface{}, error) {
	return c.call("rpush", key, element)
}

func (c *Client) Set(key string, value string) (interface{}, error) {
	return c.call("set", key, value)
}

func (c *Client) Get(key string) (interface{}, error) {
	return c.call("get", key)
}

func (c *Client) Sadd(setName string, values ...string) (interface{}, error) {
	params := append([]string{"sadd", setName}, values...)

	return c.call(params...)
}

func (c *Client) Smembers(setName string) (interface{}, error) {
	return c.call("smembers", setName)
}

func (c *Client) SisMember(setName string, value string) (interface{}, error) {
	return c.call("sismember", setName, value)
}

func (c *Client) Srem(setName string, values ...string) (interface{}, error) {
	params := append([]string{"srem", setName}, values...)

	return c.call(params...)
}

func (c *Client) Del(key string) (interface{}, error) {
	return c.call("del", key)
}

func (c *Client) responseRead() (interface{}, error) {
	firstChar, err := c.reader.ReadByte()
	if err != nil {
		c.reset()
		return nil, err
	}

	switch firstChar {
	case ':':
		strResp, err := c.reader.ReadString('\n')
		if err != nil {
			c.reset()
			return nil, err
		}

		return strconv.Atoi(strings.TrimSpace(strResp))
	case '+':
		strResp, err := c.reader.ReadString('\n')
		if err != nil {
			c.reset()
			return nil, err
		}

		return strings.TrimSpace(strResp), nil

	case '$':
		size, err := c.reader.ReadString('\n')
		if err != nil {
			c.reset()
			return nil, err
		}

		if size == "0" {
			return "", nil
		}

		if size == "-1" {
			return nil, nil
		}

		strResp, err := c.reader.ReadString('\n')
		if err != nil {
			c.reset()
			return nil, err
		}

		return strings.TrimSpace(strResp), nil

	case '*':
		size, err := c.reader.ReadString('\n')

		sizeInt, err := strconv.Atoi(strings.TrimSpace(size))
		if err != nil {
			c.reset()
			return nil, err
		}

		if sizeInt == 0 {
			return make([]interface{}, 0), nil
		}

		if sizeInt == -1 {
			return nil, nil
		}

		result := make([]interface{}, sizeInt)
		for i := 0; i < sizeInt; i++ {
			next, err := c.responseRead()
			if err != nil {
				c.reset()
				return nil, err
			}

			result[i] = next
		}

		return result, nil

	case '-':
		errMessage, err := c.reader.ReadString('\n')

		if err != nil {
			c.reset()
			return nil, err
		}

		return nil, fmt.Errorf(strings.TrimSpace(errMessage))
	default:
		c.reset()

		return nil, fmt.Errorf("Unexpected output")
	}
}
