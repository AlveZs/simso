import os

class SchedLogger():
    def __init__(self):
        self.rows = []
        self.tree_height = 0

    def add_row(self, message):
        """
        Add an information row to log.
        """
        self.rows.append(message)

    def save(self, file):
        """
        Save the log file in logs directory.
        """
        create_directories()
        with open("./logs/%s" % file, 'w') as f:
            f.write("\n".join(self.rows))

def create_directories():
    """
    Creates the necessary directories to save the generated task sets files.
    """
    if not os.path.isdir('./logs'):
        os.makedirs('./logs/')

sched_logger = SchedLogger()
