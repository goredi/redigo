// Copyright 2024 Steven Hartland
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

// connectionState represents the state of a connection.
type connectionState uint16

// Connection states flags.
// If you make a change to this you also need to update:
// * Command action generation in command_action_gen.go
// * TestPoolCloseCleanup
// * activeConn.reset()
const (
	stateClientNoEvict       connectionState = 1 << iota // Not evictable.
	stateClientNoTouch                                   // LRU/LFU stats touch disabled.
	stateClientReplyOff                                  // Connection replies aren't being sent.
	stateClientReplySkip                                 // Replies aren't expected for this cmd.
	stateClientReplySkipNext                             // Set stateClientReplySkip for next cmd.
	stateClientTracking                                  // Key tracking is enabled.
	stateMonitor                                         // Monitoring server commands.
	stateMulti                                           // Processing a transaction.
	statePsubscribe                                      // Potentially subscribed to a pattern.
	stateReadOnly                                        // Read only mode.
	stateSsubscribe                                      // Potentially subscribed to a shard channel.
	stateSubscribe                                       // Potentially subscribed to a channel.
	stateWatch                                           // Watching one or more key.
)

// commandAction represents an action to be taken when processing a command.
type commandAction struct {
	// Action specifies the action to be taken.
	Action func(cs *connectionState)

	// Next specifies sub actions for given arguments.
	Next map[string]*commandAction
}

// update updates the connection state based on the command and arguments if needed.
func (cs *connectionState) update(info map[string]*commandAction, first bool, arg0 string, args ...interface{}) {
	if first {
		if *cs&stateClientReplySkipNext != 0 {
			*cs &^= stateClientReplySkipNext
			*cs |= stateClientReplySkip
		} else {
			*cs &^= stateClientReplySkip
		}
	}

	ci, ok := info[arg0]
	if !ok {
		// No match.
		return
	}

	if ci.Action != nil {
		// Match with no more args.
		ci.Action(cs)
		return
	}

	if len(args) == 0 {
		// No match due to lack of args.
		return
	}

	arg0, ok = args[0].(string)
	if !ok {
		// No match due type miss-match.
		return
	}

	// Check the next argument.
	cs.update(ci.Next, false, arg0, args[1:]...)
}
