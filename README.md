
Lock-Free Fixed Persistent Sorted and Elastic Hash Map (FFPE)

Steps to compare Concurrent Hash Maps (CHM), Concurrent Skip Lists
(CSL), Concurrent Tries (CT) and FFP (S-Sorted and E-Elastic) models
in Linux using a terminal emulator:

1. git clone git@github.com:miar/ffpe.git

2. cd ffpe/benchSuite/

3. Open the Makefile file, update JDK_PATH and JDK with your system
values, close Makefile and do 'make' in the terminal

4. Results with execution times and memory used by each model will
appear in the 'tmp' directory

5. If you want to see how our model is implemented check the 'ffpo',
'ffps' and 'ffpe' directories.

Enjoy it.