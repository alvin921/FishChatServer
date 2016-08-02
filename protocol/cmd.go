//
// Copyright 2014 Hong Miao (miaohong@miaohong.org). All Rights Reserved.
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

package protocol

const (
	DEV_TYPE_WATCH  = "D"
	DEV_TYPE_CLIENT = "C"
)

// status of p2p msg
const (
	P2P_ACK_FALSE   = "FALSE"   // msg server received
	P2P_ACK_SENT    = "SENT"    // sent
	P2P_ACK_REACHED = "REACHED" // msg reach the peer(Send2ID)
	P2P_ACK_READ    = "READ"    // receiver read this msg
)

const (
	RSP_SUCCESS = "SUCCESS"
	RSP_ERROR   = "ERROR"
)

const (
	//SEND_PING
	SEND_PING_CMD = "PING"

	REQ_LOGIN_CMD = "REQ_LOGIN"
	RSP_LOGIN_CMD = "RSP_LOGIN"
	/*
	         device/client -> gateway
	   		REQ_LOGIN_CMD
	               arg0: ClientID        //用户ID
	               arg1: ClientType    //终端类型"C" or "D"，是client还是device
	               arg2: ClientPwd     //nil for Device/password for Client
	         gateway -> device/client
	   		RSP_LOGIN_CMD
	               arg0: SUCCESS/ERROR
	               arg1: uuid
	               arg2: MsgServerAddr
	         device/client -> MsgServer
	   		REQ_LOGIN_CMD
	               arg0: ClientID        //用户ID
	               arg1: uuid
	         MsgServer -> device/client
	   		RSP_LOGIN_CMD
	               arg0: SUCCESS/ERROR
	*/
	REQ_LOGOUT_CMD = "REQ_LOGOUT"
	RSP_LOGOUT_CMD = "RSP_LOGOUT"
	/*
	   device/client -> MsgServer
	       REQ_LOGOUT_CMD

	   MsgServer -> device/client
	       RSP_LOGOUT_CMD
	       arg0: SUCCESS/ERROR
	*/

	REQ_SEND_P2P_MSG_CMD     = "REQ_SEND_P2P_MSG"
	IND_SEND_P2P_MSG_CMD     = "IND_SEND_P2P_MSG"
	RSP_SEND_P2P_MSG_CMD     = "RSP_SEND_P2P_MSG"
	ROUTE_SEND_P2P_MSG_CMD   = "ROUTE_SEND_P2P_MSG"
	IND_ACK_P2P_STATUS_CMD   = "IND_ACK_P2P_STATUS"
	ROUTE_ACK_P2P_STATUS_CMD = "ROUTE_ACK_P2P_STATUS"
	/*
	   device/client -> MsgServer
	       REQ_SEND_P2P_MSG_CMD
	       arg0: Sent2ID       //接收方用户ID
	       arg1: Msg           //消息内容

	       IND_ACK_P2P_STATUS_CMD
	       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
	       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读
	       arg2: fromID        //发送方用户ID

	   返回给消息发送者的消息
	   MsgServer -> device/client
	       RSP_SEND_P2P_MSG_CMD
	       arg0: SUCCESS/FAILED
	       arg1: uuid // MsgServer分配的消息uuid，发送方根据此uuid确定该消息状态

	       IND_ACK_P2P_STATUS_CMD
	       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
	       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读

	   通过Router转发消息(对终端开发者不可见)
	   MsgServer -> Router
	       REQ_SEND_P2P_MSG_CMD
	       arg0: Sent2ID       //接收方用户ID
	       arg1: Msg           //消息内容
	       arg2: FromID        //发送方用户ID
	       arg3: uuid          //MsgServer分配的消息uuid

	       IND_ACK_P2P_STATUS_CMD
	       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
	       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读
	       arg2: fromID        //发送方用户ID

	   Router -> MsgServer
	       ROUTE_SEND_P2P_MSG_CMD
	       arg0: Sent2ID       //接收方用户ID
	       arg1: Msg           //消息内容
	       arg2: FromID        //发送方用户ID
	       arg3: uuid          //MsgServer分配的消息uuid

	       ROUTE_ACK_P2P_STATUS_CMD
	       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
	       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读

	   发送给消息接受者的消息
	   MsgServer -> device/client
	       IND_SEND_P2P_MSG_CMD
	       arg0: Msg           //消息内容
	       arg1: FromID        //发送方用户ID
	       arg2: uuid          //MsgServer分配的消息uuid，可选，如果提供了则须IND_ACK_P2P_MSG_CMD(ClientID, uuid)

	       IND_ACK_P2P_STATUS_CMD
	       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
	       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读
	*/

	REQ_CREATE_TOPIC_CMD = "REQ_CREATE_TOPIC"
	RSP_CREATE_TOPIC_CMD = "RSP_CREATE_TOPIC"
	/*
	   client -> MsgServer
	       REQ_CREATE_TOPIC_CMD
	       arg0: TopicName     //群组名
	       arg1: ClientName    //用户在Topic中的Name, 比如老爸/老妈

	   MsgServer -> client
	       RSP_CREATE_TOPIC_CMD
	       arg0: SUCCESS/ERROR
	*/

	REQ_ADD_2_TOPIC_CMD = "REQ_ADD_2_TOPIC"
	RSP_ADD_2_TOPIC_CMD = "RSP_ADD_2_TOPIC"
	/*

		client -> MsgServer
		  REQ_ADD_2_TOPIC_CMD
		    arg0: TopicName     //群组名
		    arg1: NewClientID          //用户ID
		    arg2: NewClientName    //用户在Topic中的Name, 对于device, 可以是儿子/女儿

		MsgServer -> client
		  RSP_ADD_2_TOPIC_CMD
		    arg0: SUCCESS/ERROR
			arg1: TopicName
			arg2: ClientID
			arg3: ClientType
	*/

	REQ_KICK_TOPIC_CMD = "REQ_KICK_TOPIC"
	RSP_KICK_TOPIC_CMD = "RSP_KICK_TOPIC"
	/*
	    client -> MsgServer
	       REQ_KICK_TOPIC_CMD
	       arg0: TopicName     //群组名
	       arg1: NewClientID   //待移除的成员用户ID

	   MsgServer -> client
	       RSP_KICK_TOPIC_CMD
	       arg0: SUCCESS/ERROR
	*/

	REQ_JOIN_TOPIC_CMD = "REQ_JOIN_TOPIC"
	RSP_JOIN_TOPIC_CMD = "RSP_JOIN_TOPIC"
	/*
	   client -> MsgServer
	       REQ_JOIN_TOPIC_CMD
	       arg0: TopicName     //群组名
	       arg1: ClientName    //用户在Topic中的Name, 比如老爸/老妈

	   MsgServer -> client
	       RSP_JOIN_TOPIC_CMD
	       arg0: SUCCESS/ERROR
	*/

	REQ_QUIT_TOPIC_CMD = "REQ_QUIT_TOPIC"
	RSP_QUIT_TOPIC_CMD = "RSP_QUIT_TOPIC"
	/*
	   client -> MsgServer
	       REQ_QUIT_TOPIC_CMD
	       arg0: TopicName     //群组名

	   MsgServer -> client
	       RSP_QUIT_TOPIC_CMD
	       arg0: SUCCESS/ERROR
	*/

	REQ_SEND_TOPIC_MSG_CMD   = "REQ_SEND_TOPIC_MSG"
	RSP_SEND_TOPIC_MSG_CMD   = "RSP_SEND_TOPIC_MSG"
	ROUTE_SEND_TOPIC_MSG_CMD = "ROUTE_SEND_TOPIC_MSG"
	IND_SEND_TOPIC_MSG_CMD   = "IND_SEND_TOPIC_MSG"
	/*
	   device/client -> MsgServer -> Router
	       REQ_SEND_TOPIC_MSG_CMD
	       arg0: Msg           //消息内容
	       arg1: TopicName     //群组名, device无须提供

	   返回给消息发送者的消息
	   MsgServer -> device/client
	       RSP_SEND_TOPIC_MSG_CMD
	       arg0: SUCCESS/FAILED

	   通过Router转发消息(对终端开发者不可见)
	   Router -> MsgServer
	       ROUTE_SEND_TOPIC_MSG_CMD
	       arg0: Msg           //消息内容
	       arg1: TopicName     //群组名
	       arg2: ClientID      //发送方用户ID
	       arg3: ClientType    //发送方终端类型，是client还是device

	   发送给消息接受者的消息
	   MsgServer -> device/client
	       IND_SEND_TOPIC_MSG_CMD
	       arg0: Msg           //消息内容
	       arg1: TopicName     //群组名
	       arg2: ClientID      //发送方用户ID
	       arg3: ClientType    //发送方终端类型，是client还是device
	*/

	REQ_GET_TOPIC_LIST_CMD = "REQ_GET_TOPIC_LIST"
	RSP_GET_TOPIC_LIST_CMD = "RSP_GET_TOPIC_LIST"
	/*
	   device/client -> MsgServer
	       REQ_GET_TOPIC_LIST_CMD

	   MsgServer -> device/client
	       RSP_GET_TOPIC_LIST_CMD
	       arg0: SUCCESS/ERROR
	       arg1: TopicNum     // topic数目，后面跟随该数目的TopicName
	       arg2: TopicName1
	       arg3: TopicName2
	       arg4: TopicName3
	*/

	REQ_GET_TOPIC_PROFILE_CMD = "REQ_GET_TOPIC_PROFILE"
	RSP_GET_TOPIC_PROFILE_CMD = "RSP_GET_TOPIC_PROFILE"

	/*
		   device/client -> MsgServer
			  	REQ_GET_TOPIC_MEMBER_CMD
			   	arg0: TopicName
			    如果ClientID不是TopicName的成员，则返回失败

		   MsgServer -> device/client
			   	RSP_GET_TOPIC_MEMBER_CMD
			   	arg0: SUCCESS/ERROR
				// followed by topic profile
				// topicName
				// creator
				// members[]
	*/

)

const (
	REQ_MSG_SERVER_CMD = "REQ_MSG_SERVER"
	//SELECT_MSG_SERVER_FOR_CLIENT msg_server_ip
	SELECT_MSG_SERVER_FOR_CLIENT_CMD = "SELECT_MSG_SERVER_FOR_CLIENT"
)

const (
	//SEND_CLIENT_ID CLIENT_ID
	SEND_CLIENT_ID_CMD = "SEND_CLIENT_ID"
	//SEND_CLIENT_ID_FOR_TOPIC ID
	SEND_CLIENT_ID_FOR_TOPIC_CMD = "SEND_CLIENT_ID_FOR_TOPIC"
	//SUBSCRIBE_CHANNEL channelName
	SUBSCRIBE_CHANNEL_CMD = "SUBSCRIBE_CHANNEL"
	//SEND_MESSAGE_P2P send2ID send2msg
	SEND_MESSAGE_P2P_CMD = "SEND_MESSAGE_P2P"
	//RESP_MESSAGE_P2P  msg fromID uuid
	RESP_MESSAGE_P2P_CMD  = "RESP_MESSAGE_P2P"
	ROUTE_MESSAGE_P2P_CMD = "ROUTE_MESSAGE_P2P"
	CREATE_TOPIC_CMD      = "CREATE_TOPIC"
	//JOIN_TOPIC TOPIC_NAME CLIENT_ID
	JOIN_TOPIC_CMD            = "JOIN_TOPIC"
	LOCATE_TOPIC_MSG_ADDR_CMD = "LOCATE_TOPIC_MSG_ADDR"
	SEND_MESSAGE_TOPIC_CMD    = "SEND_MESSAGE_TOPIC"
	RESP_MESSAGE_TOPIC_CMD    = "RESP_MESSAGE_TOPIC"
)

const (
	//P2P_ACK clientID uuid
	P2P_ACK_CMD = "P2P_ACK"
)

const (
	CACHE_SESSION_CMD = "CACHE_SESSION"
	CACHE_TOPIC_CMD   = "CACHE_TOPIC"
)

const (
	STORE_SESSION_CMD = "STORE_SESSION"
	STORE_TOPIC_CMD   = "STORE_TOPIC"
)

type Cmd interface {
	GetCmdName() string
	ChangeCmdName(newName string)
	GetArgs() []string
	AddArg(arg string)
	ParseCmd(msglist []string)
	GetAnyData() interface{}
}

type CmdSimple struct {
	CmdName string
	Args    []string
}

func NewCmdSimple(cmdName string) *CmdSimple {
	return &CmdSimple{
		CmdName: cmdName,
		Args:    make([]string, 0),
	}
}

func (self *CmdSimple) GetCmdName() string {
	return self.CmdName
}

func (self *CmdSimple) ChangeCmdName(newName string) {
	self.CmdName = newName
}

func (self *CmdSimple) GetArgs() []string {
	return self.Args
}

func (self *CmdSimple) AddArg(arg string) {
	self.Args = append(self.Args, arg)
}

func (self *CmdSimple) ParseCmd(msglist []string) {
	self.CmdName = msglist[1]
	self.Args = msglist[2:]
}

func (self *CmdSimple) GetAnyData() interface{} {
	return nil
}

type CmdInternal struct {
	CmdName string
	Args    []string
	AnyData interface{}
}

func NewCmdInternal(cmdName string, args []string, anyData interface{}) *CmdInternal {
	return &CmdInternal{
		CmdName: cmdName,
		Args:    args,
		AnyData: anyData,
	}
}

func (self *CmdInternal) ParseCmd(msglist []string) {
	self.CmdName = msglist[1]
	self.Args = msglist[2:]
}

func (self CmdInternal) GetCmdName() string {
	return self.CmdName
}

func (self *CmdInternal) ChangeCmdName(newName string) {
	self.CmdName = newName
}

func (self CmdInternal) GetArgs() []string {
	return self.Args
}

func (self *CmdInternal) AddArg(arg string) {
	self.Args = append(self.Args, arg)
}

func (self *CmdInternal) SetAnyData(a interface{}) {
	self.AnyData = a
}

func (self CmdInternal) GetAnyData() interface{} {
	return self.AnyData
}

type CmdMonitor struct {
	SessionNum uint64
}

func NewCmdMonitor() *CmdMonitor {
	return &CmdMonitor{}
}

type ClientIDCmd struct {
	CmdName  string
	ClientID string
}

type SendMessageP2PCmd struct {
	CmdName string
	ID      string
	Msg     string
}
