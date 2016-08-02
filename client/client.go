//
// Copyright 2014 Hong Miao. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/oikomi/FishChatServer/common"
	"github.com/oikomi/FishChatServer/libnet"
	"github.com/oikomi/FishChatServer/log"
	"github.com/oikomi/FishChatServer/protocol"
)

var InputConfFile = flag.String("conf_file", "client.json", "input conf file name")

func init() {
	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", "false")
}

func heartBeat(cfg *ClientConfig, msgServerClient *libnet.Session) {
	hb := common.NewHeartBeat("client", msgServerClient, cfg.HeartBeatTime, cfg.Expire, 10)
	hb.Beat()
}

var bExit bool = false

type Client struct {
	cfg        *ClientConfig
	bLogin     bool
	clientID   string
	clientType string
	clientPwd  string
	uuid       string
	msAddr     string
	session    *libnet.Session
}

func NewClient() *Client {
	cfg, err := LoadConfig(*InputConfFile)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	return &Client{
		cfg:    cfg,
		bLogin: false,
	}
}

type HelpInfo struct {
	desc   string
	detail string
	f      func(client *Client, args []string) error
}

var help_string map[string]HelpInfo

// return (ClientID, MsgServerAddr)
func login_gateway(self *Client) error {
	fmt.Println("req GatewayServer...")

	gatewayClient, err := libnet.Dial("tcp", self.cfg.GatewayServer)
	if err != nil {
		panic(err)
	}

	cmd := protocol.NewCmdSimple(protocol.REQ_LOGIN_CMD)
	cmd.AddArg(self.clientID)
	cmd.AddArg(self.clientType)
	cmd.AddArg(self.clientPwd)

	err = gatewayClient.Send(libnet.Json(cmd))
	if err != nil {
		log.Error(err.Error())
	}

	var c protocol.CmdSimple
	err = gatewayClient.ProcessOnce(func(msg *libnet.InBuffer) error {
		log.Info(string(msg.Data))
		err = json.Unmarshal(msg.Data, &c)
		if err != nil {
			log.Error("error:", err)
		}
		return nil
	})
	if err != nil {
		log.Error(err.Error())
	}

	gatewayClient.Close()

	fmt.Println("req GatewayServer end...")

	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		log.Errorf("login gateway error: %s", c.GetArgs()[0])
		return errors.New(c.GetArgs()[0])
	}
	self.uuid = c.GetArgs()[1]
	self.msAddr = c.GetArgs()[2]

	return nil
}

func login_server(self *Client) error {
	var err error

	self.session, err = libnet.Dial("tcp", self.msAddr)
	if err != nil {
		panic(err)
	}

	fmt.Println("req to login msg server...")
	cmd := protocol.NewCmdSimple(protocol.REQ_LOGIN_CMD)
	cmd.AddArg(self.clientID)
	cmd.AddArg(self.uuid)

	err = self.session.Send(libnet.Json(cmd))
	if err != nil {
		log.Error(err.Error())
		return err
	}

	var c protocol.CmdSimple
	err = self.session.ProcessOnce(func(msg *libnet.InBuffer) error {
		log.Info(string(msg.Data))
		err = json.Unmarshal(msg.Data, &c)
		if err != nil {
			log.Error("error:", err)
		}
		return nil
	})
	if err != nil {
		log.Error(err.Error())
	}
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		log.Errorf("login msgserver error: %s", c.GetArgs()[0])
		return errors.New(c.GetArgs()[0])
	}

	fmt.Println("login msg server SUCCESS")
	return nil
}

func cmd_logout(self *Client, args []string) error {
	if self.bLogin == true {
		cmd := protocol.NewCmdSimple(protocol.REQ_LOGOUT_CMD)
		err := self.session.Send(libnet.Json(cmd))
		self.bLogin = false
		return err
	}
	return nil
}

func cmd_exit(self *Client, args []string) error {
	if self != nil && self.session != nil {
		self.session.Close()
	}
	bExit = true
	return nil
}

func cmd_delete(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	fmt.Println("Not implemented yet.")
	return nil
}

func cmd_topic(self *Client, args []string) error {
	// get topic list
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_GET_TOPIC_LIST_CMD)
	err := self.session.Send(libnet.Json(cmd))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

func cmd_topic_rsp(self *Client, c *protocol.CmdSimple) error {
	var num int
	var err error

	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	if num, err = strconv.Atoi(c.GetArgs()[1]); err != nil {
		fmt.Println(err.Error())
		log.Error(err.Error())
		return err
	}
	fmt.Println("GET_TOPIC_LIST returns (" + c.GetArgs()[1] + "): ")
	index := 0
	for {
		if index == num {
			break
		} else {
			fmt.Println(c.GetArgs()[2+index])
			index++
		}
	}

	return nil
}

func cmd_topic_profile(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 2 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_GET_TOPIC_PROFILE_CMD)
	cmd.AddArg(args[1])
	err := self.session.Send(libnet.Json(cmd))

	return err
}

func cmd_topic_profile_rsp(self *Client, c *protocol.CmdSimple) error {
	var num int

	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	fmt.Println("GET_TOPIC_PROFILE returns : ")
	fmt.Println("    topicName: " + c.GetArgs()[1])
	fmt.Println("    creator: " + c.GetArgs()[2])
	fmt.Println("    members[]: ")
	num = (len(c.GetArgs()) - 3) / 3
	index := 0
	for {
		if index == num {
			break
		} else {
			fmt.Println("        " + c.GetArgs()[3+3*index] + "\t, " + c.GetArgs()[3+3*index+1] + "\t, " + c.GetArgs()[3+3*index+2])
			index++
		}
	}

	return nil
}

func cmd_new(self *Client, args []string) error {
	// CREATE TOPIC
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 3 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_CREATE_TOPIC_CMD)
	cmd.AddArg(args[1])
	cmd.AddArg(args[2])

	err := self.session.Send(libnet.Json(cmd))
	if err != nil {
		log.Error(err.Error())
	}
	return err
}

func cmd_new_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	return nil
}

func cmd_join(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 3 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_JOIN_TOPIC_CMD)
	cmd.AddArg(args[1])
	cmd.AddArg(args[2])

	err := self.session.Send(libnet.Json(cmd))
	if err != nil {
		fmt.Errorf(err.Error())
	}
	return err
}

func cmd_join_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	return nil
}

func cmd_quit(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 2 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_QUIT_TOPIC_CMD)
	cmd.AddArg(args[1])

	err := self.session.Send(libnet.Json(cmd))
	return err
}

func cmd_quit_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	return nil
}

// add <topic> <id> <alias>
func cmd_add(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 4 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_ADD_2_TOPIC_CMD)
	cmd.AddArg(args[1])
	cmd.AddArg(args[2])
	cmd.AddArg(args[3])

	err := self.session.Send(libnet.Json(cmd))
	return err
}

func cmd_add_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	return nil
}

// kick <topic> <id>
func cmd_kick(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 3 {
		return common.SYNTAX_ERROR
	}
	cmd := protocol.NewCmdSimple(protocol.REQ_KICK_TOPIC_CMD)
	cmd.AddArg(args[1])
	cmd.AddArg(args[2])

	err := self.session.Send(libnet.Json(cmd))
	return err
}

func cmd_kick_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] != protocol.RSP_SUCCESS {
		return errors.New(c.GetArgs()[0])
	}
	return nil
}

// sendto <id> <msg>
func cmd_sendto(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if len(args) != 3 {
		return common.SYNTAX_ERROR
	}

	cmd := protocol.NewCmdSimple(protocol.REQ_SEND_P2P_MSG_CMD)
	cmd.AddArg(args[1])
	cmd.AddArg(args[2])

	err := self.session.Send(libnet.Json(cmd))
	return err
}

func cmd_sendto_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	if c.GetArgs()[0] == protocol.RSP_SUCCESS {
		fmt.Println("uuid: " + c.GetArgs()[1])
	}
	return nil
}

// [msg, fromID, uuid]
func incoming_p2p_msg(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetArgs()[1] + "  says : " + c.GetArgs()[0])
	if len(c.GetArgs()) >= 3 {
		cmd := protocol.NewCmdSimple(protocol.IND_ACK_P2P_STATUS_CMD)
		cmd.AddArg(c.GetArgs()[2])
		cmd.AddArg(protocol.P2P_ACK_READ)
		cmd.AddArg(c.GetArgs()[1])

		err := self.session.Send(libnet.Json(cmd))
		if err != nil {
			log.Error(err.Error())
		}
	}
	return nil
}

// send <msg>[ <topic>]
func cmd_send(self *Client, args []string) error {
	if self.bLogin == false {
		fmt.Println("NOT login yet. Please login first.")
		return nil
	}
	if self.clientType == protocol.DEV_TYPE_CLIENT {
		if len(args) != 3 {
			return common.SYNTAX_ERROR
		}
	} else if len(args) != 2 {
		return common.SYNTAX_ERROR
	}

	cmd := protocol.NewCmdSimple(protocol.REQ_SEND_TOPIC_MSG_CMD)
	cmd.AddArg(args[1])

	if self.clientType == protocol.DEV_TYPE_CLIENT {
		cmd.AddArg(args[2])
	}

	err := self.session.Send(libnet.Json(cmd))
	return err
}

func cmd_send_rsp(self *Client, c *protocol.CmdSimple) error {
	fmt.Println(c.GetCmdName() + " returns: " + c.GetArgs()[0])
	return nil
}

func incoming_topic_msg(self *Client, c *protocol.CmdSimple) error {
	msg := c.GetArgs()[0]
	topicName := c.GetArgs()[1]
	fromID := c.GetArgs()[2]
	fromType := c.GetArgs()[3]
	fmt.Println("Topic message received :")
	fmt.Println("    TopicName :" + topicName)
	fmt.Println("    FromID    :" + fromID)
	fmt.Println("    FromType  :" + fromType)
	fmt.Println("    Message   :" + msg)
	return nil
}

func cmd_login(self *Client, args []string) error {
	if self.bLogin {
		fmt.Println("You are already login, pls logout first")
		return nil
	}
	if len(args) != 3 && len(args) != 4 {
		return common.SYNTAX_ERROR
	}
	if args[2] != "D" && args[2] != "d" && args[2] != "C" && args[2] != "c" {
		return common.SYNTAX_ERROR
	}

	self.clientID = args[1]

	if args[2] == "D" || args[2] == "d" {
		self.clientType = protocol.DEV_TYPE_WATCH
	} else if args[2] == "C" || args[2] == "c" {
		self.clientType = protocol.DEV_TYPE_CLIENT
	} else {
	}

	self.clientPwd = ""
	if self.clientType == protocol.DEV_TYPE_CLIENT {
		if len(args) != 4 {
			return common.SYNTAX_ERROR
		}
		self.clientPwd = args[3]
	}
	// Load config
	fmt.Println("config file:" + *InputConfFile)

	fmt.Println("config file loaded")
	err := login_gateway(self)
	if err != nil {
		panic(err)
	}

	err = login_server(self)
	if err != nil {
		panic(err)
	}

	self.bLogin = true

	go heartBeat(self.cfg, self.session)

	var c protocol.CmdSimple
	go self.session.Process(func(msg *libnet.InBuffer) error {
		fmt.Println(string(msg.Data))
		err = json.Unmarshal(msg.Data, &c)
		if err != nil {
			log.Error("error:", err)
		}

		fmt.Println("msg received is : " + c.GetCmdName())
		switch c.GetCmdName() {

		case protocol.RSP_GET_TOPIC_LIST_CMD:
			cmd_topic_rsp(self, &c)

		case protocol.RSP_GET_TOPIC_PROFILE_CMD:
			cmd_topic_profile_rsp(self, &c)

		case protocol.RSP_CREATE_TOPIC_CMD:
			cmd_new_rsp(self, &c)

		case protocol.RSP_JOIN_TOPIC_CMD:
			cmd_join_rsp(self, &c)

		case protocol.RSP_QUIT_TOPIC_CMD:
			cmd_quit_rsp(self, &c)

		case protocol.RSP_ADD_2_TOPIC_CMD:
			cmd_add_rsp(self, &c)

		case protocol.RSP_KICK_TOPIC_CMD:
			cmd_kick_rsp(self, &c)

		case protocol.RSP_SEND_P2P_MSG_CMD:
			cmd_sendto_rsp(self, &c)

		case protocol.IND_ACK_P2P_STATUS_CMD:
			fmt.Println("msg sent [uuid=" + c.GetArgs()[0] + "] status: " + c.GetArgs()[1])

		case protocol.IND_SEND_P2P_MSG_CMD:
			incoming_p2p_msg(self, &c)

		case protocol.IND_SEND_TOPIC_MSG_CMD:
			incoming_topic_msg(self, &c)
		}

		return nil
	})
	return nil
}

func Help(self *Client, args []string) error {
	if len(args) <= 1 {
		help := "RDA Watch Client.\n" +
			"Usage:<cmd> [<arg0> <arg1> ...]\n" +
			"<cmd> can be:\n"
		for k, v := range help_string {
			help += k + "\t----\t" + v.desc
		}
		help += `please type "help <cmd>" to get help for specific command.` + "\n"
		fmt.Print(help)
	} else {
		if v, ok := help_string[args[1]]; ok {
			fmt.Print(v.desc)
			fmt.Print(v.detail)
		}
	}
	return nil
}

func main() {
	flag.Parse()

	client := NewClient()

	help_string = map[string]HelpInfo{
		"help": {
			"RDA Watch Client.\n",
			"",
			Help,
		},
		"login": HelpInfo{
			"Login RDA Watch Server\n",
			"login <id> <type> <pwd>\n" +
				"<type>: \"D\" for watch, \"C\" for app client\n" +
				"NOTE: watch doesn't need to provide <pwd>\n",
			cmd_login,
		},
		"logout": HelpInfo{
			"Logout from RDA Watch Server\n",
			"logout\n",
			cmd_logout,
		},
		"exit": {
			"Close this program and exit\n",
			"exit\n",
			cmd_exit,
		},
		"topic": {
			"Get your own topic list\n",
			"topic\n",
			cmd_topic,
		},
		"new": {
			"create a new topic\n",
			"new <topic_name> <alias>\n",
			cmd_new,
		},

		"delete": {
			"delete a topic, only for topic creator\n",
			"delete <topic_name>\n",
			cmd_delete,
		},
		"profile": {
			"Get specific topic profile. You MUST be a member of the topic.\n",
			"profile <topic_name>\n",
			cmd_topic_profile,
		},
		"add": {
			"Add someone into the topic, only for topic creator\n",
			"add <topic> <id> <alias>\n",
			cmd_add,
		},
		"kick": {
			"Kick someone out of the topic, only for topic creator\n",
			"kick <topic> <id>\n",
			cmd_kick,
		},
		"join": {
			"join a topic\n",
			"join <topic> <alias>\n",
			cmd_join,
		},
		"quit": {
			"Quit from a topic. Usage:\n",
			"quit <topic_name>\n",
			cmd_quit,
		},
		"send": {
			"send topic message\n",
			"send <msg>[ <topic>]\n",
			cmd_send,
		},
		"sendto": {
			"send p2p message\n",
			"sendto <id> <msg>\n",
			cmd_sendto,
		},
	}

	Help(client, nil)
	r := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Command> ")
		b, _, _ := r.ReadLine()
		line := string(b)

		tokens := strings.Split(line, " ")

		if v, ok := help_string[tokens[0]]; ok {
			ret := v.f(client, tokens)
			if ret == common.SYNTAX_ERROR {
				fmt.Printf("Syntax error, pls type \"help %s\" to get more information\n", tokens[0])
			} else if ret != nil {
				fmt.Println(ret.Error())
			}
			if bExit {
				break
			}
		} else {
			fmt.Println("Unknown command:", tokens[0])
		}
	}
}
