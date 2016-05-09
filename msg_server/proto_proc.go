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

package main

import (
	"flag"
	"strconv"

	"github.com/oikomi/FishChatServer/base"
	"github.com/oikomi/FishChatServer/common"
	"github.com/oikomi/FishChatServer/libnet"
	"github.com/oikomi/FishChatServer/log"
	"github.com/oikomi/FishChatServer/protocol"
	"github.com/oikomi/FishChatServer/storage/mongo_store"
	"github.com/oikomi/FishChatServer/storage/redis_store"
)

func init() {
	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", "false")
}

type ProtoProc struct {
	msgServer *MsgServer
}

func NewProtoProc(msgServer *MsgServer) *ProtoProc {
	return &ProtoProc{
		msgServer: msgServer,
	}
}

func (self *ProtoProc) procSubscribeChannel(cmd protocol.Cmd, session *libnet.Session) {
	log.Info("procSubscribeChannel")
	channelName := cmd.GetArgs()[0]
	cUUID := cmd.GetArgs()[1]
	log.Info(channelName)
	if self.msgServer.channels[channelName] != nil {
		self.msgServer.channels[channelName].Channel.Join(session, nil)
		self.msgServer.channels[channelName].ClientIDlist = append(self.msgServer.channels[channelName].ClientIDlist, cUUID)
	} else {
		log.Warning(channelName + " is not exist")
	}

	log.Info(self.msgServer.channels)
}

func (self *ProtoProc) procPing(cmd protocol.Cmd, session *libnet.Session) error {
	//log.Info("procPing")
	cid := session.State.(*base.SessionState).ClientID
	self.msgServer.scanSessionMutex.Lock()
	defer self.msgServer.scanSessionMutex.Unlock()
	self.msgServer.sessions[cid].State.(*base.SessionState).Alive = true

	return nil
}

/*
   发送给消息接受者的消息
   MsgServer -> device/client
       REQ_SEND_P2P_MSG_CMD
       arg0: Msg           //消息内容
       arg1: FromID        //发送方用户ID
       arg2: uuid          //MsgServer分配的消息uuid，可选，如果提供了则须IND_ACK_P2P_MSG_CMD(ClientID, uuid)
*/
func (self *ProtoProc) procOfflineMsg(session *libnet.Session, ID string) error {
	var err error
	exist, err := self.msgServer.offlineMsgCache.IsKeyExist(ID)
	if exist.(int64) == 0 {
		return err
	} else {
		omrd, err := common.GetOfflineMsgFromOwnerName(self.msgServer.offlineMsgCache, ID)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		for _, v := range omrd.MsgList {
			resp := protocol.NewCmdSimple(protocol.REQ_SEND_P2P_MSG_CMD)
			resp.AddArg(v.Msg)
			resp.AddArg(v.FromID)
			resp.AddArg(v.Uuid)

			self.msgServer.p2pAckStatus[v.FromID][v.Uuid] = protocol.P2P_ACK_SENT

			if self.msgServer.sessions[ID] != nil {
				self.msgServer.sessions[ID].Send(libnet.Json(resp))
				if err != nil {
					log.Error(err.Error())
					return err
				}
			}
		}

		omrd.ClearMsg()
		self.msgServer.offlineMsgCache.Set(omrd)
	}

	return err
}

/*
         device/client -> MsgServer
   		REQ_LOGIN_CMD
               arg0: ClientID        //用户ID
               arg1: uuid
         MsgServer -> device/client
   		RSP_LOGIN_CMD
               arg0: SUCCESS/ERROR
*/

func (self *ProtoProc) procLogin(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procLogin")
	var err error
	ClientID := cmd.GetArgs()[0]
	uuid := cmd.GetArgs()[1]
	resp := protocol.NewCmdSimple(protocol.RSP_LOGIN_CMD)

	// for cache data
	sessionCacheData, err := self.msgServer.sessionCache.Get(ClientID)
	if err != nil {
		log.Warningf("no ID : %s", ClientID)
	} else if sessionCacheData.ID != uuid {
		log.Warningf("ID(%s) & uuid(%s) not matched", ClientID, uuid)
		err = NOT_LOGIN
	}
	if err == nil {
		resp.AddArg(protocol.RSP_SUCCESS)
	} else {
		resp.AddArg(protocol.RSP_ERROR)
	}
	err2 := session.Send(libnet.Json(resp))
	if err2 != nil {
		log.Error(err2.Error())
		return err2
	}
	if err != nil {
		return err
	}
	// update the session cache
	sessionCacheData.ClientAddr = session.Conn().RemoteAddr().String()
	sessionCacheData.MsgServerAddr = self.msgServer.cfg.LocalIP
	common.StoreData(self.msgServer.sessionCache, sessionCacheData)

	log.Info(sessionCacheData)

	self.msgServer.procOffline(ClientID)
	self.msgServer.procOnline(ClientID)

	/*
		args := make([]string, 0)
		args = append(args, cmd.GetArgs()[0])
		CCmd := protocol.NewCmdInternal(protocol.CACHE_SESSION_CMD, args, sessionCacheData)

		log.Info(CCmd)

		if self.msgServer.channels[protocol.SYSCTRL_CLIENT_STATUS] != nil {
			_, err = self.msgServer.channels[protocol.SYSCTRL_CLIENT_STATUS].Channel.Broadcast(libnet.Json(CCmd))
			if err != nil {
				log.Error(err.Error())
				return err
			}
		}

		// for store data
		sessionStoreData := mongo_store.SessionStoreData{ID, session.Conn().RemoteAddr().String(),
			self.msgServer.cfg.LocalIP, true}

		log.Info(sessionStoreData)
		args = make([]string, 0)
		args = append(args, cmd.GetArgs()[0])
		CCmd = protocol.NewCmdInternal(protocol.STORE_SESSION_CMD, args, sessionStoreData)

		log.Info(CCmd)

		if self.msgServer.channels[protocol.STORE_CLIENT_INFO] != nil {
			_, err = self.msgServer.channels[protocol.STORE_CLIENT_INFO].Channel.Broadcast(libnet.Json(CCmd))
			if err != nil {
				log.Error(err.Error())
				return err
			}
		}
	*/

	self.msgServer.sessions[ClientID] = session
	self.msgServer.sessions[ClientID].State = base.NewSessionState(true, ClientID, sessionCacheData.ClientType)

	err = self.procOfflineMsg(session, ClientID)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}

/*
   device/client -> MsgServer
       REQ_LOGOUT_CMD
       arg0: ClientID        //用户ID

   MsgServer -> device/client
       RSP_LOGOUT_CMD
       arg0: SUCCESS/ERROR
*/

func (self *ProtoProc) procLogout(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procLogout")
	var err error
	ClientID := cmd.GetArgs()[0]

	resp := protocol.NewCmdSimple(protocol.RSP_LOGOUT_CMD)
	resp.AddArg(protocol.RSP_SUCCESS)

	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	self.msgServer.procOffline(ClientID)

	self.msgServer.sessionCache.Delete(ClientID)

	return err
}

/*
   IND_ACK_P2P_MSG_CMD
   arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
   arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读
*/
func (self *ProtoProc) procP2PAckStatus(fromID string, uuid string, status int) error {
	log.Info("procP2PAck")
	//var err error

	self.msgServer.p2pAckMutex.Lock()
	defer self.msgServer.p2pAckMutex.Unlock()

	self.msgServer.p2pAckStatus[fromID][uuid] = status

	return nil
	//TODO : in fact, msg sender maybe offline of different msg server, so p2pAckStatus should be in REDIS CACHE
	/*
		if status == protocol.P2P_ACK_FALSE {
			return nil
		}

		sessionCacheData, err := common.GetSessionFromCID(self.msgServer.sessionCache, fromID)
		if sessionCacheData == nil {
			log.Warningf("no cache ID : %s, err: %s", fromID, err.Error())
			sessionStoreData, err := common.GetSessionFromCID(self.msgServer.mongoStore, fromID)
			if sessionStoreData == nil {
				// not registered
				log.Warningf("no store ID : %s, err: %s", fromID, err.Error())
				return err
			}
			// offline
			log.Info(fromID + " | is offline")
			omrd, err := common.GetOfflineMsgFromOwnerName(self.msgServer.offlineMsgCache, fromID)
			log.Info(omrd)
			if err != nil {
				log.Error(err.Error())
				return err
			}
			if omrd == nil {
				omrd = redis_store.NewOfflineMsgCacheData(fromID)
			}
			omrd.AddMsg(redis_store.NewOfflineMsgData(protocol.IND_ACK_P2P_MSG_CMD+strconv.Itoa(status), fromID, uuid))

			err = self.msgServer.offlineMsgCache.Set(omrd)

			if err != nil {
				log.Error(err.Error())
				return err
			}
		} else {
			// online
			log.Info(fromID + " | is online")
			if sessionCacheData.MsgServerAddr == self.msgServer.cfg.LocalIP {
				log.Info("in the same server")
				resp := protocol.NewCmdSimple(protocol.IND_ACK_P2P_MSG_CMD)
				resp.AddArg(uuid)
				resp.AddArg(status)

				if self.msgServer.sessions[send2ID] != nil {
					self.msgServer.sessions[send2ID].Send(libnet.Json(resp))
					if err != nil {
						log.Error(err.Error())
						return err
					}
					self.msgServer.p2pAckStatus[fromID][uuid] = protocol.P2P_ACK_SENT
				}
			} else {
				if self.msgServer.channels[protocol.SYSCTRL_SEND] != nil {
					cmd.AddArg(fromID)
					//add uuid
					cmd.AddArg(uuid)
					_, err = self.msgServer.channels[protocol.SYSCTRL_SEND].Channel.Broadcast(libnet.Json(cmd))
					if err != nil {
						log.Error(err.Error())
						return err
					}
					self.msgServer.p2pAckStatus[fromID][uuid] = protocol.P2P_ACK_SENT
				}
			}
		}

		return nil
	*/
}

/*
   device/client -> MsgServer -> Router
       REQ_SEND_P2P_MSG_CMD
       arg0: Sent2ID       //接收方用户ID
       arg1: Msg           //消息内容

   返回给消息发送者的消息
   MsgServer -> device/client
       RSP_SEND_P2P_MSG_CMD
       arg0: SUCCESS/FAILED
       arg1: uuid // MsgServer分配的消息uuid，发送方根据此uuid确定该消息状态

       IND_ACK_P2P_MSG_CMD
       arg0: uuid // 发送方知道uuid对应的已发送的消息已送达
       arg1: SENT/READ // 发送方知道uuid对应的消息状态：已送达/已读

   通过Router转发消息(对终端开发者不可见)
   MsgServer -> Router
       REQ_SEND_P2P_MSG_CMD
       arg0: Sent2ID       //接收方用户ID
       arg1: Msg           //消息内容
       arg2: FromID        //发送方用户ID
       arg3: uuid          //MsgServer分配的消息uuid

   Router -> MsgServer
       ROUTE_SEND_P2P_MSG_CMD
       arg0: Sent2ID       //接收方用户ID
       arg1: Msg           //消息内容
       arg2: FromID        //发送方用户ID
       arg3: uuid          //MsgServer分配的消息uuid

   发送给消息接受者的消息
   MsgServer -> device/client
       REQ_SEND_P2P_MSG_CMD
       arg0: Msg           //消息内容
       arg1: FromID        //发送方用户ID
       arg2: uuid          //MsgServer分配的消息uuid，可选，如果提供了则须IND_ACK_P2P_MSG_CMD(ClientID, uuid)
*/

func (self *ProtoProc) procSendMessageP2P(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procSendMessageP2P")
	var err error
	send2ID := cmd.GetArgs()[0]
	send2Msg := cmd.GetArgs()[1]
	fromID := session.State.(*base.SessionState).ClientID
	store_session, err := self.msgServer.sessionCache.Get(send2ID)
	if err != nil {
		log.Warningf("no ID : %s", send2ID)

		return err
	}

	uuid := common.NewV4().String()
	log.Info("uuid : ", uuid)
	if self.msgServer.p2pAckStatus[fromID] == nil {
		self.msgServer.p2pAckStatus[fromID] = make(map[string]int)
	}
	self.procP2PAckStatus(fromID, uuid, protocol.P2P_ACK_FALSE)

	if self.msgServer.sessions[send2ID] == nil {
		//offline
		log.Info(send2ID + " | is offline")
		omrd, err := common.GetOfflineMsgFromOwnerName(self.msgServer.offlineMsgCache, send2ID)
		log.Info(omrd)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		if omrd == nil {
			omrd = redis_store.NewOfflineMsgCacheData(send2ID)
		}
		omrd.AddMsg(redis_store.NewOfflineMsgData(send2Msg, fromID, uuid))

		err = self.msgServer.offlineMsgCache.Set(omrd)

		if err != nil {
			log.Error(err.Error())
			return err
		}
	}

	if store_session.MsgServerAddr == self.msgServer.cfg.LocalIP {
		log.Info("in the same server")
		resp := protocol.NewCmdSimple(protocol.REQ_SEND_P2P_MSG_CMD)
		resp.AddArg(send2Msg)
		resp.AddArg(fromID)
		// add uuid
		resp.AddArg(uuid)

		if self.msgServer.sessions[send2ID] != nil {
			self.msgServer.sessions[send2ID].Send(libnet.Json(resp))
			if err != nil {
				log.Error(err.Error())
				return err
			}
			self.procP2PAckStatus(fromID, uuid, protocol.P2P_ACK_SENT)
		}
	} else {
		if self.msgServer.channels[protocol.SYSCTRL_SEND] != nil {
			cmd.AddArg(fromID)
			//add uuid
			cmd.AddArg(uuid)
			_, err = self.msgServer.channels[protocol.SYSCTRL_SEND].Channel.Broadcast(libnet.Json(cmd))
			if err != nil {
				log.Error(err.Error())
				return err
			}
			self.procP2PAckStatus(fromID, uuid, protocol.P2P_ACK_SENT)
		}
	}

	return nil
}

/*
   Router -> MsgServer
       ROUTE_SEND_P2P_MSG_CMD
       arg0: Sent2ID       //接收方用户ID
       arg1: Msg           //消息内容
       arg2: FromID        //发送方用户ID
       arg3: uuid          //MsgServer分配的消息uuid

   发送给消息接受者的消息
   MsgServer -> device/client
       REQ_SEND_P2P_MSG_CMD
       arg0: Msg           //消息内容
       arg1: FromID        //发送方用户ID
       arg2: uuid          //MsgServer分配的消息uuid，可选，如果提供了则须IND_ACK_P2P_MSG_CMD(ClientID, uuid)
*/
func (self *ProtoProc) procRouteMessageP2P(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procRouteMessageP2P")
	var err error
	send2ID := cmd.GetArgs()[0]
	send2Msg := cmd.GetArgs()[1]
	fromID := cmd.GetArgs()[2]
	uuid := cmd.GetArgs()[3]
	_, err = self.msgServer.sessionCache.Get(send2ID)
	if err != nil {
		log.Warningf("no ID : %s", send2ID)

		return err
	}

	resp := protocol.NewCmdSimple(protocol.REQ_SEND_P2P_MSG_CMD)
	resp.AddArg(send2Msg)
	resp.AddArg(fromID)
	// add uuid
	resp.AddArg(uuid)

	if self.msgServer.sessions[send2ID] != nil {
		self.msgServer.sessions[send2ID].Send(libnet.Json(resp))
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	return nil
}

/*
   REQ_CREATE_TOPIC_CMD
   arg0: TopicName     //群组名
   arg1: ClientName    //用户在Topic中的Name, 比如老爸/老妈
*/
func (self *ProtoProc) procCreateTopic(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procCreateTopic")
	var err error

	topicName := cmd.GetArgs()[0]
	ClientName := cmd.GetArgs()[1]
	ClientID := session.State.(*base.SessionState).ClientID
	ClientType := session.State.(*base.SessionState).ClientType

	resp := protocol.NewCmdSimple(protocol.RSP_CREATE_TOPIC_CMD)

	if len(cmd.GetArgs()) != 2 {
		err = SYNTAX_ERROR
	} else
	// only DEV_TYPE_CLIENT CAN create topic
	if ClientType != mongo_store.DEV_TYPE_CLIENT {
		err = DENY_ACCESS
	} else {
		// check whether the topic exist
		topicCacheData, _ := self.msgServer.topicCache.Get(topicName)
		if topicCacheData != nil {
			log.Warningf("TOPIC %s exist", topicName)
			err = TOPIC_EXIST
		} else {
			topicStoreData, _ := self.msgServer.mongoStore.GetTopicFromCid(topicName)
			if topicStoreData != nil {
				log.Warningf("TOPIC %s exist", topicName)
				err = TOPIC_EXIST
			} else {
				// create the topic store
				topicStoreData = mongo_store.NewTopicStoreData(topicName, ClientID)
				member := mongo_store.NewMember(ClientID, ClientName, ClientType)
				topicStoreData.AddMember(member)
				err = common.StoreData(self.msgServer.mongoStore, topicStoreData)
				if err != nil {
					log.Error(err.Error())
					goto ErrOut
				}
				// create the topic cache
				topicCacheData = redis_store.NewTopicCacheData(topicStoreData)
				topicCacheData.AddMember(member)
				err = common.StoreData(self.msgServer.topicCache, topicCacheData)
				if err != nil {
					log.Error(err.Error())
					goto ErrOut
				}

			}
		}

	}

ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err

	/*
		topicCacheData := redis_store.NewTopicCacheData(topicName, session.State.(*base.SessionState).ClientID,
			self.msgServer.cfg.LocalIP)

		t := protocol.NewTopic(topicName, self.msgServer.cfg.LocalIP, session.State.(*base.SessionState).ClientID, session)
		t.ClientIDList = append(t.ClientIDList, session.State.(*base.SessionState).ClientID)
		t.TSD = topicCacheData
		self.msgServer.topics[topicName] = t
		self.msgServer.topics[topicName].Channel = libnet.NewChannel(self.msgServer.server.Protocol())

		self.msgServer.topics[topicName].Channel.Join(session, nil)

		log.Info(topicCacheData)
		args := make([]string, 0)
		args = append(args, topicName)
		CCmd := protocol.NewCmdInternal(protocol.CACHE_TOPIC_CMD, args, topicCacheData)
		m := redis_store.NewMember(session.State.(*base.SessionState).ClientID)
		CCmd.AnyData.(*redis_store.TopicCacheData).MemberList = append(CCmd.AnyData.(*redis_store.TopicCacheData).MemberList, m)

		log.Info(CCmd)

		if self.msgServer.channels[protocol.SYSCTRL_TOPIC_STATUS] != nil {
			_, err = self.msgServer.channels[protocol.SYSCTRL_TOPIC_STATUS].Channel.Broadcast(libnet.Json(CCmd))
			if err != nil {
				log.Error(err.Error())
				return err
			}
		}

		// store topic
		topicStoreData := mongo_store.NewTopicStoreData(topicName, session.State.(*base.SessionState).ClientID,
			self.msgServer.cfg.LocalIP)

		args = make([]string, 0)
		args = append(args, topicName)
		CCmd = protocol.NewCmdInternal(protocol.STORE_TOPIC_CMD, args, topicStoreData)
		member := mongo_store.NewMember(session.State.(*base.SessionState).ClientID)
		CCmd.AnyData.(*mongo_store.TopicStoreData).MemberList = append(CCmd.AnyData.(*mongo_store.TopicStoreData).MemberList, member)

		log.Info(CCmd)

		if self.msgServer.channels[protocol.STORE_TOPIC_INFO] != nil {
			_, err = self.msgServer.channels[protocol.STORE_TOPIC_INFO].Channel.Broadcast(libnet.Json(CCmd))
			if err != nil {
				log.Error(err.Error())
				return err
			}
		}
	*/
	return nil
}

/*
   client -> MsgServer
       REQ_ADD_2_TOPIC_CMD
       arg0: TopicName     //群组名
       arg1: NewClientID          //用户ID
       arg2: NewClientName    //用户在Topic中的Name, 对于device, 可以是儿子/女儿

   MsgServer -> client
       RSP_ADD_2_TOPIC_CMD
       arg0: SUCCESS/FAILED
*/
func (self *ProtoProc) procAdd2Topic(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procAdd2Topic")
	var err error

	topicName := cmd.GetArgs()[0]
	mID := cmd.GetArgs()[1]
	mName := cmd.GetArgs()[2]
	ClientID := session.State.(*base.SessionState).ClientID
	ClientType := session.State.(*base.SessionState).ClientType

	resp := protocol.NewCmdSimple(protocol.RSP_ADD_2_TOPIC_CMD)

	if len(cmd.GetArgs()) != 3 {
		err = SYNTAX_ERROR
	} else
	// only DEV_TYPE_CLIENT CAN create topic
	if ClientType != mongo_store.DEV_TYPE_CLIENT {
		err = DENY_ACCESS
	} else {
		// check whether the topic exist
		topicCacheData, _ := self.msgServer.topicCache.Get(topicName)
		if topicCacheData == nil {
			log.Warningf("TOPIC %s not exist", topicName)
			err = TOPIC_NOT_EXIST
		} else
		// only topic creater can do this
		if topicCacheData.CreaterID != ClientID {
			log.Warningf("ClientID %s is not creater of topic %s", ClientID, topicName)
			err = DENY_ACCESS
		} else {
			// New Member MUST be online
			sessionCacheData, err := self.msgServer.sessionCache.Get(mID)
			if sessionCacheData == nil {
				log.Warningf("Client %s not online", mID)
				err = NOT_ONLINE
			} else
			// Watch can only be added in ONE topic
			if sessionCacheData.ClientType == mongo_store.DEV_TYPE_WATCH && len(sessionCacheData.TopicList) != 1 {
				log.Warningf("Watch %s is in topic %s", mID, sessionCacheData.TopicList[0])
				err = DENY_ACCESS
			} else {
				// sessioin cache and store
				sessionCacheData.AddTopic(topicName)
				if err = self.msgServer.sessionCache.Set(sessionCacheData); err != nil {
					log.Error(err.Error())
				} else if err = self.msgServer.mongoStore.Set(sessionCacheData.SessionStoreData); err != nil {
					log.Error(err.Error())
				} else {
					// topic cache and store
					member := mongo_store.NewMember(mID, mName, sessionCacheData.ClientType)
					topicCacheData.AddMember(member)
					err = self.msgServer.topicCache.Set(topicCacheData)
					if err != nil {
						log.Error(err.Error())
						goto ErrOut
					}
					err = self.msgServer.mongoStore.Set(topicCacheData.TopicStoreData)
					if err != nil {
						log.Error(err.Error())
						goto ErrOut
					}
				}
			}
		}
	}

ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

/*
   client -> MsgServer
       REQ_KICK_TOPIC_CMD
       arg0: TopicName     //群组名
       arg1: NewClientID   //待移除的成员用户ID

   MsgServer -> client
       RSP_KICK_TOPIC_CMD
       arg0: SUCCESS/FAILED
*/
func (self *ProtoProc) procKickTopic(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procKickTopic")
	var err error
	var topicCacheData *redis_store.TopicCacheData
	var sessionCacheData *redis_store.SessionCacheData
	var sessionStoreData *mongo_store.SessionStoreData

	topicName := cmd.GetArgs()[0]
	mID := cmd.GetArgs()[1]

	ClientID := session.State.(*base.SessionState).ClientID
	ClientType := session.State.(*base.SessionState).ClientType

	resp := protocol.NewCmdSimple(protocol.RSP_KICK_TOPIC_CMD)

	if len(cmd.GetArgs()) != 2 {
		err = SYNTAX_ERROR
		goto ErrOut
	}

	// only DEV_TYPE_CLIENT CAN do this
	if ClientType != mongo_store.DEV_TYPE_CLIENT {
		err = DENY_ACCESS
		goto ErrOut
	}

	// check whether the topic exist
	topicCacheData, err = self.msgServer.topicCache.Get(topicName)
	if topicCacheData == nil {
		log.Warningf("TOPIC %s not exist", topicName)
		err = TOPIC_NOT_EXIST
		goto ErrOut
	}

	// only topic creater can do this
	if topicCacheData.CreaterID != ClientID {
		log.Warningf("ClientID %s is not creater of topic %s", ClientID, topicName)
		err = DENY_ACCESS
		goto ErrOut
	}

	if !topicCacheData.MemberExist(mID) {
		log.Warningf("member %s is not in topic %s", mID, topicName)
		err = NOT_MEMBER
		goto ErrOut
	}
	// update topic cache and store
	topicCacheData.RemoveMember(mID)
	err = self.msgServer.topicCache.Set(topicCacheData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}
	err = self.msgServer.mongoStore.Set(topicCacheData.TopicStoreData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}

	// update session cache and store
	sessionStoreData, err = self.msgServer.mongoStore.GetSessionFromCid(mID)
	if sessionStoreData == nil {
		log.Warningf("ID %s not registered", mID)
	} else {
		log.Warningf("remove topic from store of Client %s", mID)
		sessionStoreData.RemoveTopic(topicName)
		err = self.msgServer.mongoStore.Set(sessionStoreData)
		if err != nil {
			log.Error(err.Error())
			goto ErrOut
		}
		sessionCacheData, err = self.msgServer.sessionCache.Get(mID)
		if sessionCacheData != nil {
			log.Warningf("remove topic from cache of Client %s", mID)
			sessionCacheData.RemoveTopic(topicName)
			err = self.msgServer.sessionCache.Set(sessionCacheData)
			if err != nil {
				log.Error(err.Error())
				goto ErrOut
			}
		}
	}

ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

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
       REQ_SEND_TOPIC_MSG_CMD
       arg0: Msg           //消息内容
       arg1: TopicName     //群组名
       arg2: ClientID      //发送方用户ID
       arg3: ClientType    //发送方终端类型，是client还是device
*/
func (self *ProtoProc) procSendTopicMsg(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procSendTopicMsg")
	var err error
	var topicName string
	var topicCacheData *redis_store.TopicCacheData
	var sessionCacheData *redis_store.SessionCacheData
	//var sessionStoreData *mongo_store.SessionStoreData

	msg := cmd.GetArgs()[0]
	resp := protocol.NewCmdSimple(protocol.RSP_SEND_TOPIC_MSG_CMD)

	ClientID := session.State.(*base.SessionState).ClientID
	ClientType := session.State.(*base.SessionState).ClientType

	if ClientType == mongo_store.DEV_TYPE_CLIENT {
		if len(cmd.GetArgs()) != 2 {
			err = SYNTAX_ERROR
			goto ErrOut
		}
	} else if len(cmd.GetArgs()) != 1 {
		err = SYNTAX_ERROR
		goto ErrOut
	}

	// get session cache
	sessionCacheData, err = self.msgServer.sessionCache.Get(ClientID)
	if sessionCacheData == nil {
		log.Errorf("ID %s cache missing", ClientID)
		err = NOT_ONLINE
		goto ErrOut
	} else if ClientType == mongo_store.DEV_TYPE_WATCH {
		topicName = sessionCacheData.GetTopics()[0]
	} else {
		topicName = cmd.GetArgs()[1]
	}

	// check whether the topic exist
	topicCacheData, err = self.msgServer.topicCache.Get(topicName)
	if topicCacheData == nil {
		log.Warningf("TOPIC %s not exist", topicName)
		err = TOPIC_NOT_EXIST
	} else {
		topic_msg_resp := protocol.NewCmdSimple(cmd.GetCmdName())
		topic_msg_resp.AddArg(msg)
		topic_msg_resp.AddArg(topicName)
		topic_msg_resp.AddArg(ClientID)
		topic_msg_resp.AddArg(ClientType)

		if topicCacheData.AliveMemberNumMap[self.msgServer.cfg.LocalIP] > 0 {
			// exactly in this server, just broadcasting
			log.Warningf("topic %s has %d member(s) in this server", topicName, topicCacheData.AliveMemberNumMap[self.msgServer.cfg.LocalIP])
			for _, mID := range topicCacheData.MemberList {
				if self.msgServer.sessions[mID.ID] != nil {
					self.msgServer.sessions[mID.ID].Send(libnet.Json(topic_msg_resp))
					if err != nil {
						log.Fatalln(err.Error())
					}
				}
			}
		}
		if self.msgServer.channels[protocol.SYSCTRL_SEND] != nil {
			topic_msg_resp.ChangeCmdName(protocol.ROUTE_SEND_TOPIC_MSG_CMD)
			for ip, num := range topicCacheData.AliveMemberNumMap {
				if num > 0 {
					log.Warningf("topic %s has %d member(s) in ip %s", topicName, num, ip)
					if ip != self.msgServer.cfg.LocalIP {
						// not in this server, routing it
						_, err = self.msgServer.channels[protocol.SYSCTRL_SEND].Channel.Broadcast(libnet.Json(topic_msg_resp))
						if err != nil {
							log.Error(err.Error())
						}
						break
					}
				}
			}
		}
	}

ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

/*
   通过Router转发消息(对终端开发者不可见)
   Router -> MsgServer
       ROUTE_SEND_TOPIC_MSG_CMD
       arg0: Msg           //消息内容
       arg1: TopicName     //群组名
       arg2: ClientID      //发送方用户ID
       arg3: ClientType    //发送方终端类型，是client还是device
*/
func (self *ProtoProc) procRouteTopicMsg(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procRouteTopicMsg")
	var err error

	//send2Msg := cmd.GetArgs()[0]
	topicName := cmd.GetArgs()[1]
	//fromID := cmd.GetArgs()[2]
	//fromType := cmd.GetArgs()[3]

	// check whether the topic exist
	topicCacheData, err := self.msgServer.topicCache.Get(topicName)
	if topicCacheData == nil {
		log.Warningf("TOPIC %s not exist: %s", topicName, err.Error())
		return TOPIC_NOT_EXIST
	}

	cmd.ChangeCmdName(protocol.REQ_SEND_TOPIC_MSG_CMD)

	// exactly in this server, just broadcasting
	log.Warningf("topic %s has %d member(s) in this server", topicName, topicCacheData.AliveMemberNumMap[self.msgServer.cfg.LocalIP])
	for _, mID := range topicCacheData.MemberList {
		if self.msgServer.sessions[mID.ID] != nil {
			self.msgServer.sessions[mID.ID].Send(libnet.Json(cmd))
			if err != nil {
				log.Fatalln(err.Error())
			}
		}
	}

	return nil
}

func (self *ProtoProc) procJoinTopic(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procJoinTopic")
	var err error
	var topicCacheData *redis_store.TopicCacheData
	var sessionCacheData *redis_store.SessionCacheData
	//	var sessionStoreData *mongo_store.SessionStoreData
	var member *mongo_store.Member

	topicName := cmd.GetArgs()[0]
	clientName := cmd.GetArgs()[1]

	clientID := session.State.(*base.SessionState).ClientID
	clientType := session.State.(*base.SessionState).ClientType

	resp := protocol.NewCmdSimple(protocol.RSP_JOIN_TOPIC_CMD)

	if len(cmd.GetArgs()) != 2 {
		err = SYNTAX_ERROR
		goto ErrOut
	}

	// check whether the topic exist
	topicCacheData, err = self.msgServer.topicCache.Get(topicName)
	if topicCacheData == nil {
		log.Warningf("TOPIC %s not exist", topicName)
		err = TOPIC_NOT_EXIST
		goto ErrOut
	}

	if topicCacheData.MemberExist(clientID) {
		log.Warningf("ClientID %s exists in topic %s", clientID, topicName)
		err = MEMBER_EXIST
		goto ErrOut
	}

	sessionCacheData, err = self.msgServer.sessionCache.Get(clientID)
	if sessionCacheData == nil {
		log.Warningf("Client %s not online", clientID)
		err = NOT_ONLINE
		goto ErrOut
	}
	// Watch can only be added in ONE topic
	if clientType == mongo_store.DEV_TYPE_WATCH && len(sessionCacheData.TopicList) != 1 {
		log.Warningf("Watch %s is in topic %s", clientID, sessionCacheData.TopicList[0])
		err = DENY_ACCESS
		goto ErrOut

	}

	// sessioin cache and store
	sessionCacheData.AddTopic(topicName)
	err = self.msgServer.sessionCache.Set(sessionCacheData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}
	err = self.msgServer.mongoStore.Set(sessionCacheData.SessionStoreData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}

	// topic cache and store
	member = mongo_store.NewMember(clientID, clientName, clientType)
	topicCacheData.AddMember(member)
	err = self.msgServer.topicCache.Set(topicCacheData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}
	err = self.msgServer.mongoStore.Set(topicCacheData.TopicStoreData)
	if err != nil {
		log.Error(err.Error())
		goto ErrOut
	}

ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

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

func (self *ProtoProc) procGetTopicList(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procGetTopicList")
	var err error

	resp := protocol.NewCmdSimple(protocol.RSP_GET_TOPIC_LIST_CMD)

	clientID := session.State.(*base.SessionState).ClientID
	//clientType := session.State.(*base.SessionState).ClientType

	sessionCacheData, err := self.msgServer.sessionCache.Get(clientID)
	if sessionCacheData == nil {
		log.Warningf("Client %s not online", clientID)
		err = NOT_ONLINE
		goto ErrOut
	}
ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
		resp.AddArg(strconv.Itoa(len(sessionCacheData.TopicList)))
		for _, topic := range sessionCacheData.TopicList {
			resp.AddArg(topic)
		}
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

/*
   device/client -> MsgServer
       REQ_GET_TOPIC_MEMBER_CMD
       arg0: TopicName
   如果ClientID不是TopicName的成员，则返回失败

   返回给消息发送者的消息
   MsgServer -> device/client
       RSP_GET_TOPIC_MEMBER_CMD
       arg0: SUCCESS/FAILED
       arg1: MemberNum     // topic member数目，后面跟随该数目的member
       arg2: Member1
       arg3: Member2
       arg4: Member3
*/

func (self *ProtoProc) procGetTopicMember(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procGetTopicMember")
	var err error

	resp := protocol.NewCmdSimple(protocol.RSP_GET_TOPIC_MEMBER_CMD)

	topicName := cmd.GetArgs()[0]

	clientID := session.State.(*base.SessionState).ClientID
	//clientType := session.State.(*base.SessionState).ClientType

	// check whether the topic exist
	topicCacheData, err := self.msgServer.topicCache.Get(topicName)
	if topicCacheData == nil {
		log.Warningf("TOPIC %s not exist", topicName)
		err = TOPIC_NOT_EXIST
		goto ErrOut
	}
	if topicCacheData.MemberExist(clientID) == false {
		log.Warningf("%s not the member of topic %d", clientID, topicName)
		err = DENY_ACCESS
		goto ErrOut
	}
ErrOut:
	if err != nil {
		resp.AddArg(err.Error())
	} else {
		resp.AddArg(protocol.RSP_SUCCESS)
		resp.AddArg(strconv.Itoa(len(topicCacheData.MemberList)))
		for _, member := range topicCacheData.MemberList {
			resp.AddArg(member.ID)
			resp.AddArg(member.Name)
		}
	}
	err = session.Send(libnet.Json(resp))
	if err != nil {
		log.Error(err.Error())
	}

	return err
}

// not a good idea
func (self *ProtoProc) procP2pAck(cmd protocol.Cmd, session *libnet.Session) error {
	log.Info("procP2pAck")
	var err error
	clientID := cmd.GetArgs()[0]
	uuid := cmd.GetArgs()[1]
	status, err := strconv.Atoi(cmd.GetArgs()[2])
	self.msgServer.p2pAckMutex.Lock()
	defer self.msgServer.p2pAckMutex.Unlock()

	//self.msgServer.p2pAckStatus[clientID][uuid] = true

	err = self.procP2PAckStatus(clientID, uuid, status)

	return err
}
