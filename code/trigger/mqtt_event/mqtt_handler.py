# ----------------------------------------------------------------------
#
#    Shoestring Analysis Engine
#
#    Copyright (C) 2025  Shoestring and University of Cambridge
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, version 3 of the License.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see https://www.gnu.org/licenses/.
#
# ----------------------------------------------------------------------


import logging

logger = logging.getLogger(__name__)
from .simple_tree import SimpleTreeNode


class MQTTHandler:
    def __init__(self):
        self.topic_map = SimpleTreeNode()
        self.msg_queue = []
        self.subscriptions = []

    def register_function_to_topic(self, topic, func):
        self.subscriptions.append(topic)
        topic_tokens = topic.split("/")
        return add_to_tree(topic_tokens, self.topic_map, func)

    def add_msg(self, topic, payload):
        self.msg_queue.append({"topic": topic, "payload": payload})

    def get_functions_for_topic(self, topic):
        topic_tokens = topic.split("/")
        result = recursive_search(topic_tokens, self.topic_map)
        return result

    async def call_functions_for_messages(self,config):
        for msg in self.msg_queue:
            topic = msg["topic"]
            payload = msg["payload"]
            function_set = self.get_functions_for_topic(topic)
            for func in function_set:
                await func(topic, payload, config=config)
        self.msg_queue = []
        
    def has_entries(self):
        return self.topic_map.size() > 0


def recursive_search(tokens, current_tree_level: SimpleTreeNode):
    function_set = set()
    if len(tokens) == 0:
        # logger.debug(f"No more tokens, returning current node value {current_tree_level.getValue()}")
        function_set.update(current_tree_level.getValue())
        return function_set

    token = tokens[0]

    if token in current_tree_level:
        # logger.debug(f">{token} at level")
        function_subset = recursive_search(tokens[1:], current_tree_level[token])
        if function_subset is not None:
            function_set.update(function_subset)

    if "+" in current_tree_level:
        # logger.debug(f">'+' at level for {token}")
        function_subset = recursive_search(tokens[1:], current_tree_level["+"])
        if function_subset is not None:
            function_set.update(function_subset)

    if "#" in current_tree_level:
        # logger.debug(f">'#' at level for {token}, returning {current_tree_level['#'].getValue()}")
        function_set.update(current_tree_level["#"].getValue())

    return function_set


# build tree on add - uid is a function
def add_to_tree(tokens, current_level: SimpleTreeNode, func):
    token = tokens.pop(0)
    if len(tokens) == 0 or token == "#":  # if no more tokens or wildcard
        if token not in current_level:
            current_level.createNode(token)
        current_level[token].getValue().add(func)
    else:
        if token not in current_level:
            current_level.createNode(token)
        # recurse at next level down
        add_to_tree(tokens, current_level[token], func)
