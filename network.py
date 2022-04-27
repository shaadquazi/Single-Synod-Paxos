import math
from collections import deque
from typing import List, Union
from unittest import result
from config import CONSENSUS_REACHED, EVENTS
from model import Computers, Proposers, Acceptors
from model import MessageType, Prepare, Promise, Accept, Accepted, Rejected, Message

class Event:
    def __init__(self, tick: int, failure: List[Computers], recovery: List[Computers], request: Computers, value: int) -> None:
        self.tick = tick
        self.failure = failure
        self.recovery = recovery
        self.request = request
        self.proposedValue = value

    def __str__(self) -> str:
        result = f'T = {self.tick}; '
        if self.failure:
            result += f'Failed: {len(self.failure)};'
        if self.recovery:
            result += f'Recovered: {len(self.recovery)};'
        if self.request:
            result += f'Req By: {self.request}; Value: {self.proposedValue}'
        return result

class Network:
    def __init__(self, noOfProposers: int, noOfAcceptors: int) -> None:
        self.network = deque()
        self.proposers = [Proposers(i+1) for i in range(noOfProposers)]
        self.acceptors = [Acceptors(i+1) for i in range(noOfAcceptors)]

    def __len__(self) -> int:
        return len(self.network)

    def getComputers(self, computerType: Union[Proposers, Acceptors] = None) -> List:
        if computerType:
            return self.proposers if computerType == Proposers else self.acceptors
        return self.proposers + self.acceptors

    def getComputerById(self, computerType: Union[Proposers, Acceptors], id: int) -> Computers:
        lookThrough = self.getComputers(computerType)
        for computer in lookThrough:
                if computer.id == id:
                    return computer
        return None

    def getComputerByN(self, computerType: Union[Proposers, Acceptors], n: int) -> Computers:
        lookThrough = self.getComputers(computerType)
        for computer in lookThrough:
                if computer.getN() == n:
                    return computer
        return None

    def QueueMessage(self, message: Message) -> None:
        # Adds message m to the end of the queue.
        self.network.append(message)

    def ExtractMessage(self) -> Message:
        # Finds the first message m in the queue such that m.src.failed = false and m.dst.failed = false. 
        # If such a message exists, m is removed from N and returned as the result of Extract-Message. 
        # If no such message exists, a null value is returned.
        for message in self.network:
            if message.type == MessageType.PROPOSE:
                self.network.remove(message)
                return message
            if not message.source.failed and not message.destination.failed:
                self.network.remove(message)
                return message
        return None

    def printMessage(self, computer: Computers, message: Message):
        print(f'{message.source if message.source else "  "} -> {message.destination}\t{message}')
        return

    def DeliverMessage(self, computer: Computers, message: Message) -> None:
        # While awake, the computer can update its internal state and can call Queue-Message any number of times, 
        # but it cannot call Extract-Message.

        try:            
            if message.type == MessageType.PROPOSE:
                for acceptor in self.acceptors:
                    prepare = Prepare(computer, acceptor)
                    self.QueueMessage(prepare)
                return
            
            if message.type == MessageType.PREPARE:                
                if computer.getN() <= message.source.getN():
                    message.source.saveMessage(message)
                    computer.setN(message.source.getN())
                    promise = Promise(computer, message.source)                
                    self.QueueMessage(promise)                    
                else:
                    # This means that acceptor has seen n which is greater than proposer's n.
                    # print(f'DROPED', end=" | ")                    
                    rejected = Rejected(computer, message.source)
                    self.QueueMessage(rejected)
                return
                        
            if message.type == MessageType.PROMISE:    
                if computer.getN() == message.source.getN():
                    if message.source.getValue():
                        computer.setValue(message.source.getValue())

                    message.source.saveMessage(message)                    
                    if computer.hasMajority(MessageType.PROMISE):
                        computer.addVote(MessageType.PROMISE, message.source)
                        return
                    computer.addVote(MessageType.PROMISE, message.source)
                    if computer.hasMajority(MessageType.PROMISE):
                        # Send an accept request to all the acceptors
                        for acceptor in self.acceptors:
                            accept = Accept(computer, acceptor)
                            self.QueueMessage(accept)                    
                    return
                elif computer.getN() > message.source.getN():
                    # This means that proposer has proposed n which is greater than acceptor's n.
                    # print(f'DROPED', end=" | ")                    
                    return
                else:
                    # This means that acceptor has seen n which is greater than proposer's n.
                    # print(f'DROPED', end=" | ")
                    return                
            
            if message.type == MessageType.ACCEPT:            
                n, value = computer.getMaxNAndValue(MessageType.PROMISE)
                if n <= message.source.getN():
                    if not value or value == -math.inf:
                        value = message.source.value
                    computer.setValue(value)
                    computer.saveMessage(message)
                    accepted = Accepted(computer, message.source)  
                    self.QueueMessage(accepted)
                    return
                elif n > message.source.getN():
                    # This means that acceptor has seen n which is greater than proposer's n.
                    # print(f'DROPED', end=" | ")
                    rejected = Rejected(computer, message.source)
                    self.QueueMessage(rejected)
                    return

            if message.type == MessageType.ACCEPTED:
                if computer.getN() == message.source.getN():
                    message.source.saveMessage(message)
                    if computer.hasMajority(MessageType.ACCEPTED):
                        computer.addVote(MessageType.ACCEPTED, message.source)
                        return
                    computer.addVote(MessageType.ACCEPTED, message.source)
                    if computer.hasMajority(MessageType.ACCEPTED):
                        result = f'{computer} has reached consensus (proposed {computer.getValue(MessageType.PROPOSE)}, accepted {computer.acceptedBy[computer.getN()][0].getValue()})'
                        CONSENSUS_REACHED.append(result)
                    return
                elif computer.getN() > message.source.getN():   
                    # This means that proposer has proposed n which is greater than acceptor's n.
                    # print(f'DROPED', end=" | ")
                    return
                else:
                    # This means that acceptor has seen n which is greater than proposer's n.
                    # print(f'DROPED', end=" | ")

                    # # We will create an new Event so that the Proposer will be triggered by
                    # # external source with a new higher n and try to reach consensus
                    if computer.hasMajority(MessageType.ACCEPTED):
                        return
                        
                    for events in EVENTS:
                        if events.request == computer:
                            return
                    EVENTS.append(Event(None, None, None, computer, computer.getValue()))
                    return
                
        except Exception as ex:
            print(f'Exception: {ex}', end=" | ")
        finally:
            self.printMessage(computer, message)
