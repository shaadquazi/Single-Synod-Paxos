from typing import List
from random import randrange
from network import Network, Event
from model import DataBase, Proposers, Propose, Acceptors
from config import NO_OF_PROPOSERS, NO_OF_ACCEPTORS, MAX_DURATION, EVENTS, CONSENSUS_REACHED

class Paxos:
    DEBUG = False
    PROPOSAL_NUMBER = 0
    def __init__(self, noOfProposers: int, noOfAcceptors: int, maxSimulationDuration: int, eventsList: List[Event]) -> None:
        # All computers are connected to the network
        self.network = Network(noOfProposers, noOfAcceptors)
        self.maxDuration = maxSimulationDuration
        self.events = eventsList
        Paxos.PROPOSAL_NUMBER = 0
        if Paxos.DEBUG:
            print(f'No of Proposers: {noOfProposers}\nNo of Acceptors: {noOfAcceptors}\nTotal Duration(Ticks): {self.maxDuration}\nTotal Events: {len(self.events)}\nMajority: {((noOfAcceptors - 1) // 2) + 1}\n')
        
    def _updateEventList(self, tick):
        if len(self.events) == 0:
            return            
        if self.events[-1].tick is None:
            delay = randrange(6)
            self.events[-1].tick = tick + delay
        return

    def _getEventAt(self, tick: int) -> Event:
        # Returns the Event at tick t
        self._updateEventList(tick)
        if self.events:
            for event in self.events:
                if event.tick == tick:
                    return event
        return Event(tick, None, None, None, None)

    def _removeEventAt(self, tick: int) -> Event:
        # Removes the Event from the self.events list
        removeIndex = -1
        if self.events:            
            for index, event in enumerate(self.events):
                if event.tick == tick:
                    removeIndex = index
                    break
        return self.events.pop(removeIndex) if removeIndex != -1 else None

    def run(self) -> None:
        # Implementation of single-instance verison of Paxos consensus algorithm. 

        # Step through all the ticks        
        for currentTick in range(self.maxDuration):            
            # If there are no pending messages or events, we can end the simulation
            if len(self.network) == 0 and len(self.events) == 0:
                self._quitPaxos()
            
            print('{:03}: '.format(currentTick), end="")
            
            # We will process the event for the current tick
            currentEvent = self._getEventAt(currentTick)

            # For a given tick, only the following can happen:
            if currentEvent:
                self._removeEventAt(currentTick)
                
                #   1. A set of machines can fail
                if currentEvent.failure:
                    print(f'** ', end="")
                    for failed in currentEvent.failure:
                        computer = self.network.getComputerById(type(failed), failed.id)
                        computer.failed = True
                        print(f'{computer} ', end="")
                    print(f'FAILS **', end="")

                #   2. A set of(previously failed) machines can recover
                if currentEvent.recovery:
                    print(f'** ', end="")
                    for recovered in currentEvent.recovery:
                        computer = self.network.getComputerById(type(recovered), recovered.id)
                        computer.failed = False
                        print(f'{computer} ', end="")
                    print(f'RECOVERS **')
                    print('{:03}: '.format(currentTick), end="")

                #   3. A single message can be delivered. Only, one computer can do any work.
                if currentEvent.request and currentEvent.proposedValue:                
                    proposer = self.network.getComputerById(Proposers, currentEvent.request.id)
                    proposer.setN(self._getGlobalProposalNumber())
                    proposer.setValue(currentEvent.proposedValue)                    
                    message = Propose(src=None, dst=proposer)
                    proposer.saveMessage(message)

                    # PROPOSE messages bypass the network and are delivered directly to the specified Proposer
                    self.network.DeliverMessage(proposer, message)
                else:                    
                    message = self.network.ExtractMessage()
                    if message:
                        if currentEvent.failure:
                            print('\n{:03}: '.format(currentTick), end="")
                        self.network.DeliverMessage(message.destination, message)
                    else:
                        print()

        print(f'Simulation Terminated! Time Over!')
        self._quitPaxos()
    
    def _getGlobalProposalNumber(self) -> int:
        Paxos.PROPOSAL_NUMBER += 1
        return Paxos.PROPOSAL_NUMBER

    def _quitPaxos(self) -> None:
        # Quit Paxos; Print all consesus that were achieved during execution
        print()
        for consensus in CONSENSUS_REACHED:
            print(consensus)
        print()
        for proposer in self.network.getComputers(Proposers):
            if proposer.getValue() and not proposer.hasMajority():
                print(f'{proposer} did not reach consensus')
        
        # Print Master Table of all the messages in the network
        if Paxos.DEBUG:
            select = ['source', 'destination', 'messageType', 'n', 'value']
            print(DataBase.select(select))
        exit()

if __name__ == '__main__':    
    # CASE 1
    # No Failures; One single Proposer proposes
    EVENTS.append(Event(0, None, None, Proposers(1), 42))
    # ---------------------------------------------------


    # CASE 2
    # Proposer 1 failes and restores after some time. And during this idel time, a different Proposer proposes a new value
    # Proposer(1) Failes at tick = 8
    EVENTS.append(Event(8, [Proposers(1)], None, None, None))

    # Proposer(2) proposes a new value at tick = 11
    EVENTS.append(Event(11, None, None, Proposers(2), 37))

    # Proposer(1) Recovers at tick = 26
    EVENTS.append(Event(26, None, [Proposers(1)], None, None))
    # ---------------------------------------------------


    # # CASE 3
    # # tick = 2; with P1 and P2; only P2 reaches consensus
    # # P1 is dropped during PROMISE
    # EVENTS.append(Event(2, None, None, Proposers(2), 55))
    # # ---------------------------------------------------

    # # CASE 4
    # # tick = 3; with P1 and P2; first P2 reaches consensus and then P1 reaches 
    # # P1 is dropped during PROMISE but still makes MAJORITY, so have to drop during ACCEPTED
    # EVENTS.append(Event(3, None, None, Proposers(2), 55))
    # # ---------------------------------------------------
        
    # # CASE 5
    # # tick = 12; with P1 and P2; both P1 and P2 reach consensus
    # EVENTS.append(Event(12, None, None, Proposers(2), 55))
    # # ---------------------------------------------------

    main = Paxos(NO_OF_PROPOSERS, NO_OF_ACCEPTORS, MAX_DURATION, EVENTS)
    main.run()
    