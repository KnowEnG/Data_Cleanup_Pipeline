import os

class Utils(object):
    def __init__(self):
        print ("Initializing Utils...")

    def createFile(self, dir_name, file_name, file_content):
        os.makedirs(dir_name, mode=0o755, exist_ok=True)
        with open(os.path.join(dir_name, file_name), "w") as f:
            f.write(file_content)

        f.close()