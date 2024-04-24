# if you're reading this, I'm sorry

# use the -v flag followed by output file to write log messages, otherwise logs are ignored
# usage: multiguess.py -v [FILE] ... e.g 'python3 multiguess.py -v output.txt'

# - KNOWN PROBLEMS -
# 1. curses will throw errors on addstr or addch calls if the terminal display window is too small. If you need this to work in small terminals windows these calls will have to be wrapped in a function that will wrap text
# 2. If a game is started soon after another game has closed, the OS may still have the port used for the listen server locked. You'll know this has happened if you press 's' to start a game and nothing happens and by a log error if you use the -v flag. The short-term solution is to wait 30 seconds or so. A possible long-term solution would be to try the port and, if unsuccessful, try the next port.
# 3. Presently a guess can be buffered after you have taken your turn - this 'could' be a desirable feature, but there is nothing in the UI that represents it as an intended option
# 4. The code is poorly organized - there is a lot of room to delete code duplication

# - UNIMPLEMENTED FEATURES - 
# 1. Despite what the UI tells you, pressing 'i' for info does nothing, it would be nice if it gave the rules of the game on-screen
# 2. Only local games are supported right now, though it wouldn't be too difficult to make this work across the internet - a lot of the infrastructure is built already but it is untested

import sys
import os
import curses
from curses import wrapper

import socket

from random import randint

IS_HOST = False

QUIT = False
FREEZE_INPUT = False

COLS = 0
ROWS = 0

OUTFILE = None
FILE = None

# This global holds essentially all data from a game session
# the update_messages would be a good place to load this data to a file for persistance
MESSAGES = []
NUMPLAYERS = 0

SECRET_NUMBER = 0

input_string = ''

class message:
    def __init__(self, msg_type, msg):
        self.msg_type = msg_type
        self.msg = msg

    def get_type(self):
        return self.msg_type

    def get_msg(self):
        return self.msg


def logToFile(message):
    global FILE
    if OUTFILE is not None:
        if FILE is None:
            try:
                FILE = open(OUTFILE, 'x') # returns error if file exists
            except:
                if os.path.exists(OUTFILE):
                    os.remove(OUTFILE)
                    FILE = open(OUTFILE, 'a')
                else:
                    print('INVALID FILE', file = sys.stderr)
        FILE.write(message + '\n')

def guessToPacket(guess):
    packet = str(guess[0]) + '$' + str(guess[1])
    return packet.encode()

def guessFromPacket(packet):
    data = packet.decode()
    fields = data.split('$')
    return (fields[0], fields[1])

def poll_input(inp_win, allowDecimal = False, isString = False):
    global input_string
    k = inp_win.getkey()
    if (k.isnumeric() or (allowDecimal and k == '.') or (isString and k.isalpha())) and not FREEZE_INPUT:
        inp_win.erase()
        input_string += k
        inp_win.addstr(0, 0, input_string)
    elif k == curses.KEY_ENTER or k == '\n':
        inp_win.erase()
        complete_string = input_string
        input_string = ''
        return complete_string
    elif k == curses.KEY_BACKSPACE or k == '\b' or ord(k) == 127:
        input_string = input_string[:-1]
        inp_win.erase()
        inp_win.addstr(0, 0, input_string)
    elif k == 'q' and not isString:
        global QUIT
        QUIT = True
    #else:
        #inp_win.addstr(0, 0, 'INVALID KEY - please enter a number and press enter to guess', curses.A_REVERSE)

def draw_horizontal_line(win, row):
    cols = win.getmaxyx()[1]
    for i in range(cols):
        win.addch(row, i, '_')

def center_text(string):
    return COLS//2 - len(string)//2

def center_row():
    return ROWS//2

def poll_socket(connections):
    for s in connections:
        s[0].setblocking(False)
        try:
            guess = guessFromPacket(s[0].recv(512))
            fromAddress = s[1]
            logToFile('message {} recved from {}'.format(guess, fromAddress))
            return guess, fromAddress
        except:
            continue
    return None

# send the message to each connection except the original sender
# connection is a 2-tuple -> (socket, (ip_address, port))
def send_message(connections, message, fromAddress = None):
    for s in connections:
        if fromAddress is None or s[1][0] is not fromAddress[0]: # check ip addr equality
            s[0].setblocking(True)
            s[0].sendall(guessToPacket(message))
            logToFile('message {} sent from {} to {}'.format(message, fromAddress, s[1]))
            s[0].setblocking(False)

def update_messages(win):
    win.erase()
    max_height = win.getmaxyx()[0]
    start = 0
    global MESSAGES
    if len(MESSAGES) > max_height:
        start = len(MESSAGES) - max_height
    index = 0
    for i in range(start, len(MESSAGES)):
        mess = MESSAGES[i]
        msg = mess.get_msg()
        if mess.get_type() == 'guess':
            win.addstr(index, 0, "{} guessed {}!".format(msg[0], msg[1]))
        elif mess.get_type() == 'update':
            win.addstr(index, 0, "Your guess of {} was {}!".format(msg[0], msg[1]))
        else:
            win.addstr(index, 0, "{} won! The number was {}!".format(msg[0], msg[1]))
        index += 1
    win.refresh()

def display_title_win(win):
    title = 'MULTIGUESS'
    win.addstr(0, center_text(title), title, curses.A_BOLD)
   
    # TODO implement an info page explaining the rules
    info_message = 'press i for info'
    win.addstr(0, 0, info_message)

    quit_message = 'press q to quit'
    win.addstr(0, COLS - len(quit_message), quit_message)

def get_ip_address():
    return 'localhost'

def get_port():
    return 8080

def start_game():
    logToFile('in start_game')
    try:
        ip_address = get_ip_address()
        port = get_port()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((ip_address, port))
        s.listen(3) # should allow backlog of up to 3 connections
        s.setblocking(False)
        logToFile('Listen server set up succeeded')
        
        return s, ip_address
    except:
        logToFile('Listen server set up failed - the port {} could still be in use from a previous game. The os should release it soon'.format(port))
        return None

# returns a list of player connections as tuples of (socket, address)
def display_start_prompt(win):
    logToFile('in display_start_prompt')
    win.clear()
    try:
        server, ip_address = start_game()
    except:
        server = None
    if not server or not ip_address:
        return None 
    prompt = 'Other players can join your game with your ip address: {}'.format(ip_address) 
    win.addstr(0, center_text(prompt), prompt, curses.A_BOLD)
    win.refresh()

    players_joined = 0
    connections = []

    wait_msg = '{} player(s) have joined'.format(players_joined)
    info_msg = 'Press p to start playing or q to quit'.format(players_joined)

    wait_win = curses.newwin(1, COLS, ROWS//2 + 1, center_text(wait_msg))
    info_win = curses.newwin(1, COLS, ROWS//2 + 2, center_text(info_msg))

    info_win.addstr(0, 0, info_msg)
    info_win.refresh()

    wait_win.nodelay(True) # getkey will now not block so we can check for connections

    global QUIT
    while not QUIT:
        wait_win.erase()

        wait_msg = '{} player(s) have joined'.format(players_joined)
        wait_win.addstr(0, 0, wait_msg, curses.A_BLINK)
        try:
            k = wait_win.getkey()
            if k == 'q':
                QUIT = True
            elif k == 'p':
                global NUMPLAYERS
                NUMPLAYERS = players_joined + 1
                global SECRET_NUMBER
                SECRET_NUMBER = get_random_number(0, 100)
                for conn in connections:
                    conn[0].setblocking(True)
                    conn[0].send(str(str(NUMPLAYERS) + '$' + str(SECRET_NUMBER)).encode())
                    conn[0].setblocking(False)
                break
        except:
            pass
        try:
            conn, addr = server.accept()
            connections.append((conn, addr))
            players_joined += 1
        except:
            pass
        wait_win.refresh()

    return connections

# returns a socket that represents a connection to the server
def join_game(ip_address):

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip_address, 8080))
        global NUMPLAYERS
        global SECRET_NUMBER

        response = s.recv(512).decode()
        fields = response.split('$')

        NUMPLAYERS = int(fields[0])
        SECRET_NUMBER = int(fields[1])

        s.setblocking(False)
        return s, (ip_address, 8080)

    except:
        logToFile('Join game failed')
        return None; 

def display_join_prompt(win):
    win.clear()
    prompt = 'Press h to join a local game or i to join over internet:'
    win.addstr(0, COLS//2 - len(prompt)//2, prompt, curses.A_STANDOUT)
    win.refresh()

    ip_address_length = 15

    inp_win = curses.newwin(1, COLS, ROWS//2 + 1, COLS//2 - ip_address_length//2)

    isLocal = False;

    inp_win.nodelay(False) # block for this input
    global QUIT
    while not QUIT:
        k = inp_win.getkey()
        if k == 'h':
            isLocal = True
            break
        elif k == 'i':
            isLocal = False
            break
        elif k == 'q':
            QUIT = True;

    if isLocal is False:
        win.clear()
        prompt = 'Enter IP address of existing game:'
        win.addstr(0, COLS//2 - len(prompt)//2, prompt, curses.A_STANDOUT)
        win.refresh()

    win.clear()
    prompt = 'Waiting for host to start the game...'
    win.addstr(0, COLS//2 - len(prompt)//2, prompt, curses.A_BLINK)
    win.refresh()

    connections = [] # so that we can treat both servers and clients the same

    while not QUIT:
        if isLocal is False:
            while not QUIT:
                ip_address = poll_input(inp_win, True)
                if ip_address and len(ip_address) == ip_address_length:
                    break
        else:
            ip_address = get_ip_address()
        if ip_address:
            connection = join_game(ip_address)
            if connection is not None: # check for a valid socket
                connections.append(connection)
                return connections

def display_lobby(win, prompt_win):
    draw_horizontal_line(win, 0)
    draw_horizontal_line(win, 3)
    welcome = 'WELCOME TO MULTIGUESS'
    info = 'Guess the random number first to win!'

    win.addstr(1, COLS//2 - len(welcome)//2, welcome, curses.A_BOLD)
    win.addstr(2, COLS//2 - len(info)//2, info, curses.A_BOLD)

    name = display_name_prompt(prompt_win)

    name_prompt = 'Welcome, {}!'.format(name)

    win.addstr(ROWS//4 + 1, COLS//2 - len(name_prompt)//2, name_prompt, curses.A_BOLD)
    win.refresh()
    
    prompt = ' Press s to start a new game or j to join an existing game '

    prompt_win.addstr(0, center_text(prompt), prompt, curses.A_STANDOUT)
    prompt_win.refresh()


    win.nodelay(True) # getkey will now not block

    global QUIT
    while not QUIT:
        try:
            k = win.getkey()
            if k == 'q':
                QUIT = True
            if k == 's':
                #start new game
                global IS_HOST
                IS_HOST = True
                connections = display_start_prompt(prompt_win)
                if connections is not None and len(connections) > 0:
                    return connections, name
            if k == 'j':
                #join game
                # This will run until user quits, comes back here,
                # or enters a valid IP address to join
                connection = display_join_prompt(prompt_win)
                if connection is not None:
                    return connection, name
        except:
            continue

# Gives a blocking prompt to the user to enter a name and returns the name 
def display_name_prompt(win):
    prompt = 'Please enter your name:'
    win.addstr(0, center_text(prompt), prompt, curses.A_BOLD)
    win.refresh()

    inp_win = curses.newwin(1, COLS, ROWS//2 + 1, COLS//2 - 5)

    while True:
        try:
            name = poll_input(inp_win, True, True)
        except:
            name = None
        if name and len(name) > 0:
            win.clear()
            inp_win.clear()
            inp_win.refresh()
            return name

def check_guess(number):
    number = int(number)
    if number < SECRET_NUMBER:
        comparison = 'too low'
    elif number > SECRET_NUMBER:
        comparison = 'too high'
    else:
        comparison = 'correct'
    return (number, comparison)

def display_end_prompt(win, connections):

    global QUIT
    global SECRET_NUMBER # we generate a new one here

    if not IS_HOST:
        prompt = 'Waiting for host to restart...'

        # hack to clear the waiting prompt
        win.addstr(ROWS - 1, 0, '                                         ', curses.A_BLINK)
        win.addstr(ROWS - 1, 0, prompt, curses.A_BLINK)
        win.refresh()

        win.nodelay(True) # getkey will now not block
        connections[0][0].setblocking(False)

        while not QUIT:
            try:
                k = win.getkey()
                if k == 'q':
                    QUIT = True
            except:
                pass
            try:
                SECRET_NUMBER = int(connections[0][0].recv(512).decode())
                # if control reaches here recv was successfull
                return True
            except:
                pass
        
    else:
        prompt = ' Press r to start a new game or q to quit '

        win.addstr(center_row(), center_text(prompt), prompt, curses.A_STANDOUT)
        win.refresh()


        prompt = 'Waiting for you to restart...'

        # hack to clear the waiting prompt
        win.addstr(ROWS - 1, 0, '                                         ', curses.A_BLINK)
        win.addstr(ROWS - 1, 0, prompt, curses.A_BLINK)
        win.refresh()

        win.nodelay(False) # getkey will now block

        while not QUIT:
            k = win.getkey()
            if k == 'q':
                QUIT = True
            if k == 'r':
                #start new game
                SECRET_NUMBER = get_random_number(0, 100)
                for connection in connections:
                    connection[0].setblocking(True)

                    connection[0].send(str(SECRET_NUMBER).encode())
                    connection[0].setblocking(False)
                return True

        return False

def get_random_number(at_least, no_more_than):
    return randint(at_least, no_more_than)

# This contains the fundamental game logic
def main(stdscr):
    stdscr.clear()

    global ROWS
    global COLS 
    ROWS = curses.LINES
    COLS = curses.COLS

    global OUTFILE
    if len(sys.argv) > 2:
        if sys.argv[1] == '-v':
           OUTFILE = str(sys.argv[2])

    logToFile('set output file')

    prompt_win = curses.newwin(1, COLS-1, ROWS//2, 0)
    connections, name = display_lobby(stdscr, prompt_win)
    del prompt_win
    
    stdscr.clear()

    display_title_win(stdscr)
    draw_horizontal_line(stdscr, 1)
    draw_horizontal_line(stdscr, ROWS - 2)

    msg_win = curses.newwin(ROWS - 5, COLS, 3, 0)

    prompt = 'Enter guess:'
    inp_win = curses.newwin(1, COLS - len(prompt), ROWS - 1, len(prompt))
    inp_win.nodelay(True)

    wait_prompt = 'Waiting for other players to guess...'

    # display prompt
    stdscr.addstr(ROWS - 1, 0, prompt, curses.A_STANDOUT)
    stdscr.refresh()

    playersTurn = True
    totalGuesses = 0
    
    global MESSAGES
    global FREEZE_INPUT

    # Game loop
    while not QUIT:
        if totalGuesses == NUMPLAYERS:
            # start the next turn
            stdscr.addstr(ROWS - 1, 0, prompt, curses.A_STANDOUT)
            stdscr.refresh()
            totalGuesses = 0
            playersTurn = True

        if playersTurn:
            # see if player has submitted a guess yet
            try:
                inp = poll_input(inp_win)
            except:
                inp = None

            if inp and len(inp) > 0: # if user has submitted a valid guess

                # create guess and update MESSAGES list
                internal_guess = ('You', inp)
                MESSAGES.append(message('guess', internal_guess))
                game_message = check_guess(inp)
                MESSAGES.append(message('update', game_message))

                # update the screen and send the guess to the server 
                update_messages(msg_win)
                send_message(connections, (name, inp))

                # check if guess is correct 
                if int(inp) == SECRET_NUMBER:
                    MESSAGES.append(message('game_over', internal_guess))
                    update_messages(msg_win)
                    FREEZE_INPUT = True
                    if display_end_prompt(stdscr, connections):
                        totalGuesses = NUMPLAYERS # hack to reuse above logic
                        MESSAGES.clear()
                        update_messages(msg_win)
                        FREEZE_INPUT = False
                        continue
                else:
                    totalGuesses += 1
                    playersTurn = False

                # change the prompt from 'Enter guess' to 'Waiting'
                stdscr.addstr(ROWS - 1, 0, wait_prompt, curses.A_BLINK)
                stdscr.refresh()


        if not FREEZE_INPUT:
            # attempt to recv guesses from other players
            try:
                external_guess, fromAddress = poll_socket(connections)
            except:
                external_guess = None

            if external_guess and len(external_guess) > 0:

                # update MESSAGES list, update screen and game logic
                MESSAGES.append(message('guess', external_guess))
                update_messages(msg_win)

                if IS_HOST:
                    # send message from host to all except the sender
                    send_message(connections, external_guess, fromAddress)

                # check if guess is correct 
                if int(external_guess[1]) == SECRET_NUMBER:
                    MESSAGES.append(message('game_over', external_guess))
                    update_messages(msg_win)
                    FREEZE_INPUT = True
                    if display_end_prompt(stdscr, connections):
                        totalGuesses = NUMPLAYERS # hack to reuse above logic
                        MESSAGES.clear()
                        update_messages(msg_win) # clear messages window

                        FREEZE_INPUT = False
                else:
                    totalGuesses += 1

        stdscr.refresh()

# From curses - prevents wonky things in the terminal if the application suddenly closes
wrapper(main)
