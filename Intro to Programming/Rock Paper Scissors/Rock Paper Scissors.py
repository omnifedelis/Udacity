#!/usr/bin/env python3

"""This program plays a game of Rock, Paper, Scissors between two Players,
and reports both Player's scores each round."""

import random
moves = ['rock', 'paper', 'scissors']

"""The Player class is the parent class for all of the Players
in this game"""


class Player:
    def move(self):
        return 'rock'

    def learn(self, my_move, their_move):
        pass


class RandomPlayer(Player):
    # randomly choose a move
    def move(self):
        return random.choice(moves)


class HumanPlayer(Player):
    # Allows for human interaction
    def move(self):
        move = (input("Enter your move > "))
        while move not in moves:
            move = input("Please Pick from rock, paper, scissors > ")
        return move


class ReflectPlayer(Player):
    # Will copy opponents last move
    def __init__(self):
        super().__init__()
        self.their_move = None

    def learn(self, my_move, their_move):
        self.my_move = my_move
        self.their_move = their_move

    def move(self):
        # randomly choose a move if it's the first round
        if self.their_move is None:
            return random.choice(moves)
        return self.their_move


class CyclePlayer(Player):
    # Will choose the next move in the list
    def __init__(self):
        super().__init__()
        self.my_move = None

    def learn(self, my_move, their_move):
        self.my_move = my_move
        self.their_move = their_move

    def move(self):
        # randomly choose a move if it's the first round
        if self.my_move is None:
            return random.choice(moves)
        elif self.my_move == "rock":
            return "paper"
        elif self.my_move == "paper":
            return "scissors"
        else:
            return "rock"


def beats(one, two):
    return ((one == 'rock' and two == 'scissors') or
            (one == 'scissors' and two == 'paper') or
            (one == 'paper' and two == 'rock'))


class Game:
    def __init__(self, p1, p2):
        self.p1 = p1
        self.p2 = p2
        self.score1 = 0
        self.score2 = 0

    def play_round(self):
        move1 = self.p1.move()
        move2 = self.p2.move()
        print(f"Player 1: {move1}  Player 2: {move2}")
        # if move1 matches move2,skip. if move1 beats move2 add point
        # to player1, else add pt to player2
        if move1 == move2:
            print(f"Score: Player One {self.score1}, Player Two {self.score2}")
            print("Round TIED!!!")
        elif beats(move1, move2):
            self.score1 += 1
            print(f"Score: Player One {self.score1}, Player Two {self.score2}")
            print("Player 1 wins round!!!")
        else:
            self.score2 += 1
            print(f"Score: Player One {self.score1}, Player Two {self.score2}")
            print("Player 2 wins round")
        self.p1.learn(move1, move2)
        self.p2.learn(move2, move1)

    def play_game(self):
        print("Lets Play Rock, Paper, Scissors!!!")
        print("Game start!")
        for round in range(3):
            print(f"Round {round+1}:")
            self.play_round()
        if self.score1 == self.score2:
                print("Game TIED!!!")
        elif self.score1 > self.score2:
            print("Player 1 wins game!!!")
        else:
            print("Player 2 wins game")
        print("Game over!")


if __name__ == '__main__':
    modes = ["1", "rock", "2", "random", "3", "reflect", "4", "cycle"]
    Intro = input("Welcome to Rock, Paper, Scissors\n would you like to play?"
                  "Y or N > ")
    if Intro == "Y" or 'y':
        match = input('Which mode type do you want to play?\n Please enter'
                      'the number or mode: 1-rock, 2-random, 3-reflect, '
                      '4-cycle? > ')

        while match not in modes:
            print("I'm sorry, that is not a valid entry\n please try again")
            quit()
        if match == '1' or 'rock':
                game = Game(HumanPlayer(), Player())
                game.play_game()
        elif match == '2' or 'random':
                game = Game(HumanPlayer(), RandomPlayer())
                game.play_game()
        elif match == '3' or 'reflect':
                game = Game(HumanPlayer(), ReflectPlayer())
                game.play_game()
        elif match == '4' or 'cycle':
                game = Game(HumanPlayer(), CyclePlayer())
                game.play_game()
    elif Intro == 'N' or 'n':
        print("Maybe next time")
        quit()
