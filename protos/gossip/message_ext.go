/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package gossip

import fmt "fmt"

func (p *Payload) toString() string {
	return fmt.Sprintf("Block message: {Data: %d bytes, seq: %d}", len(p.Data), p.SeqNum)
}

func (du *DataUpdate) toString() string {
	mType := PullMsgType_name[int32(du.MsgType)]
	return fmt.Sprintf("Type: %s, items: %d, nonce: %d", mType, len(du.Data), du.Nonce)
}

func (mr *MembershipResponse) toString() string {
	return fmt.Sprintf("MembershipResponse with Alive: %d, Dead: %d", len(mr.Alive), len(mr.Dead))
}

func (sis *StateInfoSnapshot) toString() string {
	return fmt.Sprintf("StateInfoSnapshot with %d items", len(sis.Elements))
}
