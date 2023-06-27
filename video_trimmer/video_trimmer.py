import cv2
import evadb


class ConnectionManager:
    def __init__(self):
        self.cursor = None

    def initiateConnection(self):
        self.cursor = evadb.connect().cursor()

    def saveVideo(self, file_path, file_name):
        self.cursor.load(
            file_regex=file_path,
            format="VIDEO",
            table_name=''.join(file_name.split('.')[:-1])
        ).df()

    def createUDF(self, method_name, method_path):
        self.cursor.query("DROP UDF IF EXISTS {};".format(method_name)).execute()
        self.cursor.query("CREATE UDF IF NOT EXISTS {} INPUT (seconds INTEGER) OUTPUT (timestamp NDARRAY STR(8)) TYPE NdarrayUDF IMPL '{}';".format(method_name, method_path)).execute()

def getFrameData(connectionManager, file_name, start_time, end_time):
    relation = connectionManager.cursor.table(''.join(file_name.split('.')[:-1]))
    query = relation.filter("Timestamp(seconds) >= '{}' AND Timestamp(seconds) <= '{}'".format(start_time, end_time))
    return query.df()


def save(frame_data, file_name, trimmed_file_path):
    fourcc = cv2.VideoWriter_fourcc(*'avc1')
    pixel_data = frame_data.iloc[0]["{}.data".format(''.join(file_name.split('.')[:-1]))]
    video = cv2.VideoWriter(trimmed_file_path, fourcc, 60, (len(pixel_data[0]), len(pixel_data))) #1920, 1090

    frame_data = frame_data.reset_index() 
    for index, row in frame_data.iterrows():
        video.write(row["{}.data".format(''.join(file_name.split('.')[:-1]))])

    video.release()

def getInputData():
    print('Enter the file path: ', end='')
    input_file_path = input()
    
    print('Enter the start time (HH:MM:SS): ', end='')
    start_time = input()
    
    print('Enter the end time (HH:MM:SS): ', end='')
    end_time = input()
    
    print('Enter the destination path: ', end='')
    trimmed_file_path = input()
    
    return input_file_path, start_time, end_time, trimmed_file_path


if __name__ == "__main__":
    input_file_path, start_time, end_time, trimmed_file_path = getInputData()
    """Initializtions """
    connectionManager = ConnectionManager()
    connectionManager.initiateConnection()
    connectionManager.createUDF("Timestamp", "./timestamp.py")
    
    """Core logic """
    file_name = input_file_path.strip().split('/')[-1]

    connectionManager.saveVideo(input_file_path, file_name)
    frameData = getFrameData(connectionManager, file_name, start_time, end_time)
    save(frameData, file_name, trimmed_file_path)
