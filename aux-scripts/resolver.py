#!/usr/bin/python3

import socket
import sys

print socket.getaddrinfo (sys.argv [1], 0, 0, 0, 0)
