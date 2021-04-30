<h1> Rock, Paper, Scissors Game Project Task</h1>

🌋📄✂️ Getting started
Here are some steps to get you started in the Rock Paper Scissors project! As you work through the project, make sure to test your program by running it.

<h3>1. Download the starter code</h3>

Click [here](https://s3.amazonaws.com/video.udacity-data.com/topher/2018/November/5c002226_rps-starter-code/rps-starter-code.py) to download the starter code.

The starter code gives you a place to begin, with Player and Game classes that are mostly empty. Over the course of the project, you will be greatly expanding the classes and methods in this program.

<i>Read</i> the starter code, and <i>run</i> it on your computer to see what it does.

Try importing it into the Python interpreter and experimenting with the Player and Game objects.

<h3>2. Create a player subclass that plays randomly</h3>
The starter Player class always plays 'rock'. That's not a very good strategy! Create a subclass called RandomPlayer that chooses its move at random. When you call the move method on a RandomPlayer object, it should return one of 'rock', 'paper', or 'scissors' at random.

Change the code so it plays a game between two RandomPlayer objects.

<h3>3. Keep score</h3>
The starter Game class does not keep score. It doesn't even notice which player won each round. Update the Game class so that it displays the outcome of each round, and keeps score for both players. You can use the provided beats function, which tells whether one move beats another one.

Make sure to handle ties — when both players make the same move!

<h3>4. Create a subclass for a human player.</h3>
The game is a lot more interesting if you can actually play it, instead of just watching the computer play against itself. Create a HumanPlayer subclass, whose move method asks the human user what move to make. (Take another look back at the project demo to see what this can look like!)

Set the program to play a game between HumanPlayer and RandomPlayer.

<h3>5. Create player classes that remember</h3>
At the end of each game round, the Game class calls the learn method on each player object, to tell that player what the other player's move was. This means you can have computer players that change their moves depending on what has happened earlier in the game. To do this, you will need to implement learn methods that save information into instance variables.

Create a ReflectPlayer class that remembers what move the <i>opponent</i> played last round, and plays that move <i>this</i> round. (In other words, if you play 'paper' on the first round, a ReflectPlayer will play 'paper' on the second round.)

Create a CyclePlayer class that remembers what move <i>it</i> played last round, and cycles through the different moves. (If it played 'rock' this round, it should play 'paper' in the next round.)

<i>(Something to think about: What should these classes do on the first move?)</I>

Test each of these player classes versus HumanPlayer.

<h3>6. Validate user input</h3>
The human player might sometimes make typos. If they enter roxk instead of rock, the HumanPlayer code should let them try again. (See how this works in the demo if you type something in that isn't a valid move.)

<h3>7. Announce the winner</h3>
It's up to you how long the game should run. The starter code always plays three rounds, but that's not the only way it could work. You could choose to continue until the player types quit, or you could have the game run until one player is ahead by three points, or any other rule that makes sense to you.

At the end of the game, have it print out which player won, and what the final scores are.

<h3>8. Check your code formatting</h3>
Use the pycodestyle tool to check the formatting of your code. Make the edits that it recommends, then re-run it to see fewer and fewer warnings. By the time you're done, it should display <i>no warnings or errors</i> at all.