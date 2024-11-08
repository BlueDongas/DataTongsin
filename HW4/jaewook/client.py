import socket
import threading

class Client:
    def __init__(self, host = "localhost", port = 6000):
        self.host = host
        self.port = port
        self.client_id = None
      
    def connect_to_server(self):
        # 서버와의 연결 생성
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))

        # 서버로부터 클라이언트 ID 수신
        self.client_id = int(self.client_socket.recv(1024).decode())
        #open(f"client{self.client_id}_Log.txt","w")
        print("Connect to Server")
        print(f"Receive ID to Server: {self.client_id}")
        
        ready_signal = self.client_socket.recv(1024).decode()
        if ready_signal == "READY":
            print("Start Send task to Server")
            
    def send_request(self):
        print("send_request")
        return

    def receive_chunk(self):
        print("receive_chunk")
        return

    def disconnect(self):
        return
    
if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    
    send_thread = threading.Thread(target=client.send_request)
    receive_thread = threading.Thread(target=client.receive_chunk)
    
    send_thread.start()
    receive_thread.start()
    
    send_thread.join()
    receive_thread.join()
    print("finish")
    input()