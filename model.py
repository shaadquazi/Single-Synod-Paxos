import math
from enum import Enum
from time import sleep
from config import MAJORITY
from typing import Dict, List

class MessageType(Enum):
    PROPOSE = 1
    PREPARE = 2
    PROMISE = 3
    ACCEPT = 4
    ACCEPTED = 5
    REJECTED = 6

class Computers:
    def __init__(self, id: int) -> None:
        self.id = id
        self.failed = False
        self.internalState = Storage()
        self.n = None
        self.value = None

    def setN(self, n: int) -> None:
        self.n = n

    def getN(self) -> int:
        return self.n

    def setValue(self, value: int) -> None:
        self.value = value

    def getValue(self, messageType: MessageType = None) -> int:
        if messageType:
            return self.internalState.getRecordsByMessageType(messageType)[0]['value']
        return self.value

    def saveMessage(self, message) -> None:        
        record = {
            'destination': message.destination,
            'messageType': message.type,
            'n': self.getN(),
            'value': self.getValue(),
            'messageObj': message
            }
        if message.type == MessageType.PROPOSE:
            record['source'] = None
        else:
            record['source'] = message.source
        self.internalState.addRecord(record)

    def getRecords(self) -> List:
        return self.internalState.getRecords()

    def getMaxNAndValue(self, messageType: MessageType = None) -> int:
        n = value = -math.inf
        for record in self.internalState.getRecordsByMessageType(messageType):
            if n < record['n']:
                n = record['n']
                value = record['value']
        return n, value

    def getLastAccept(self) -> Dict:
        accepts = self.internalState.getRecordsByMessageType(MessageType.ACCEPT)
        if accepts:
            return accepts[-1]
        return None

class Message:    
    def __init__(self, src: Computers, dst: Computers, messageType: MessageType) -> None:
        self.source = src
        self.destination = dst
        self.type = messageType

    def __str__(self) -> str:
        result = f'{MessageType(self.type).name.ljust(10, " ")} '

        if self.source:
            result += f'n={self.source.n} '

        if self.type == MessageType.PROMISE:
            lastAccept = self.source.getLastAccept()
            prior = None
            if lastAccept:
                n = lastAccept['n']
                value = lastAccept['value']
                prior = f'n={n}, v={value}'
            result += f'(Prior: {prior}) '

        if self.type == MessageType.ACCEPT or self.type == MessageType.ACCEPTED or self.type == MessageType.PROPOSE:
            result += f'v={self.destination.value} '
        return result

class DataBase:
    masterTable = []    
    def __init__(cls) -> None:
        pass    

    @classmethod
    def select(cls, select: List) -> str:
        result = 'MASTER TABLE'
        result += '\n'
        def round(number):
            return int(math.ceil(int(number) / 10.0)) * 10
        def createBorder():
            count = 0
            for col in select:
                count += 3 + round(len(col))
            return ''.ljust(count, "-") + '-\n'
        
        result += createBorder()
        # Print Heading                
        for col in select:
            result += f'|  {col.ljust(round(len(col)), " ")}'
        result += '|\n'

        result += createBorder()
        # Print Records
        for record in cls.masterTable:
            line = ''
            for col in select:
                if col in record:
                    line += f'|  {str(record[col]).ljust(round(len(col)), " ")}'
            line += '|\n'
            result += line
        result += createBorder()
        return result

    @classmethod
    def addRecord(cls, record: Dict) -> None:
        cls.masterTable.append(record)

    @classmethod
    def getRecordsByMessageType(cls, messageType: MessageType) -> List[Dict]:
        result = []
        for record in cls.masterTable:
            if record['messageType'] == messageType:
                result.append(record)
        return result

class Storage:
    def __init__(self) -> None:
        super().__init__()
        self.internal = []
        
    def addRecord(self, record: Dict) -> None:
        self.internal.append(record)
        DataBase.addRecord(record)

    def getRecords(self) -> List[Dict]:
        return self.internal

    def getRecordsById(self, des: int):
        pass

    def getRecordsByMessageType(self, messageType: MessageType = None) -> List[Dict]:
        if not messageType:
            return self.getRecords()

        result = []
        for record in self.getRecords():
            if record['messageType'] == messageType:
                result.append(record)
        return result

class Propose(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.PROPOSE)

class Prepare(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.PREPARE)

class Promise(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.PROMISE)

class Accept(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.ACCEPT)

class Accepted(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.ACCEPTED)

class Rejected(Message):
    def __init__(self, src: Computers, dst: Computers) -> None:
        super().__init__(src, dst, MessageType.REJECTED)  

class Acceptors(Computers):
    def __init__(self, id: int) -> None:
        super().__init__(id)
        self.setN(1)

    def __str__(self) -> str:
        return f'A{self.id}'

class Proposers(Computers):
    def __init__(self, id: int) -> None:
        super().__init__(id)
        self.consensus = False       
        self.acceptedBy = {}
        self.promisedBy = {}

    def __str__(self) -> str:
        return f'P{self.id}'

    def hasMajority(self, messageType: MessageType = None) -> bool:
        if not messageType:
            return self.consensus
        if MessageType.PROMISE == messageType:
            if self.getN() in self.promisedBy:
                return len(self.promisedBy[self.getN()]) >= MAJORITY
        elif MessageType.ACCEPTED == messageType:
            if self.getN() in self.acceptedBy:
                if len(self.acceptedBy[self.getN()]) >= MAJORITY:
                    self.consensus = True
                else:
                    self.consensus = False
                return self.consensus
        return False

    def addVote(self, messageType: MessageType, computer: Computers) -> None:
        if MessageType.PROMISE == messageType:
            if computer.getN() in self.promisedBy:
                self.promisedBy[computer.getN()].append(computer)
            else:
                self.promisedBy[computer.getN()] = [computer]
        elif MessageType.ACCEPTED == messageType:
            if computer.getN() in self.acceptedBy:
                self.acceptedBy[computer.getN()].append(computer)
            else:
                self.acceptedBy[computer.getN()] = [computer]
        return
