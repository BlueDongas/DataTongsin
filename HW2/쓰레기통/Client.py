import socket
import threading 
import time
import random
import pickle
import struct

client_id = None
log_file = None

file_list = [] #다운받을 리스트
file_list_lock = threading.Lock()

def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()
def send_result(client_socket, result,server_name):
    try:
        data_to_send = pickle.dumps((client_id,result))
        data_size = len(data_to_send)

        client_socket.sendall(struct.pack('Q', data_size))
        client_socket.sendall(data_to_send)
    except Exception as e:
        print(f"Error send data {result} to {server_name}")

def request_file(client_socket,request_list,server_name):
    try:
        while True:
            with file_list_lock:
                if not file_list:
                    print("All task complete")
                    #send_result(client_socket,"complete")
                    break
                file_number = file_list[0]
            if not request_list:
                continue
            try:
                if file_number == request_list[0]:
                    with file_list_lock:
                        file_list.pop(0) #보내야할 요소를 전체 리스트에서 뺌
                    print(f"Request file{file_number} to {server_name}")

                    # 요청 보내기
                    send_result(client_socket,file_number,server_name)
                    time.sleep(1)
                    request_list.pop(0)
                else:
                    continue # 서버로 보내야할 리스트의 요소가 전체 리스트의 첫번째 요소와 같아질때까지 continue
            except Exception as e:
                print(f"Failed to send request file{file_number} to {server_name}")
    except Exception as e:
        print(f"Failed to send request to {server_name}: {e}")


# 서버들로부터 정보를 받는 함수
def receive_file(client_socket,server_name):
    while True: 
        try:
            packed_size = b""
            while len(packed_size) < 8:  # 8바이트를 모두 수신할 때까지 반복
                packet = client_socket.recv(8 - len(packed_size))
                if not packet:
                    print("No size information received from data server. Closing connection.")
                    return None
                packed_size += packet

            data_size = struct.unpack('Q', packed_size)[0]
            
            receive_data = b""
            while len(receive_data) < data_size:
                packet = client_socket.recv(4096)
                if not packet:
                    print(f"Connection closed while receiving data from {server_name}.")
                    return
                receive_data += packet

            try:
                receive_result = pickle.loads(receive_data)
                print(f"Received data from {server_name}: {receive_result}")
            except pickle.UnpicklingError as e:
                print(f"Error unpickling data: {e}")
                break
        except Exception as e:
            print(f"Error reciveing file from {server_name} : {e}")

def connect_to_data_server(server_address, server_port, server_name, request_list):
    global client_id
    global log_file
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(20)
        client_socket.connect((server_address, server_port))
        client_id = pickle.loads(client_socket.recv(4096))
        time.sleep(4)
        start_signal = pickle.loads(client_socket.recv((4096)))
        if start_signal == "start":
            #log_file = open(f"Cache Server{cache_id}.txt", "w")

            print(f"Connected to {server_name} on port {server_port}")

            request_thread = threading.Thread(target=request_file, args=(client_socket,request_list,server_name))
            receive_thread = threading.Thread(target=receive_file, args=(client_socket,server_name))

            request_thread.start()
            receive_thread.start()
            print("good job jaewook")
            request_thread.join()
            receive_thread.join()
    finally:
        client_socket.close()
        print(f"Connection closed for client")


def connect_to_cache_server(server_address, server_port, server_name, request_list): #client handler
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_address, server_port))
        print(f"Connected to {server_name} on port {server_port}")
        
        time.sleep(2)
        start_signal = pickle.loads(client_socket.recv(4096))
        if start_signal == "start":

            request_thread = threading.Thread(target=request_file, args=(client_socket,request_list,server_name))
            receive_thread = threading.Thread(target=receive_file, args=(client_socket,server_name))

            request_thread.start()
            receive_thread.start()

            request_thread.join()
            receive_thread.join()
    finally:
        client_socket.close()
        print(f"Connection closed for client")

# 클라이언트가 데이터 서버와 2개의 캐시 서버에 각각 연결
def client():
    # 각 서버에 대한 정보를 설정
    Data_server = ('localhost', 10000, 'Data Server') #7412 이상 요청하기
    Even_Cache_server = ('localhost', 20000, 'Cache Server 1') #짝수 요청
    Odd_Cache_server = ('localhost', 30000, 'Cache Server 2')  #홀수 요청

    Odd_list = [] #홀수
    Even_list = [] #짝수 
    Data_request_list = [] #데이터 서버로 부터 요청받을 파일들 
    
    odd_cache_sum = 0
    even_cache_sum = 0
    data_sum = 0

    for _ in range(10): #테스트용 나중에 1000개로 수정
        file_number = random.randint(1,10000)

        if file_number % 2 == 0:
            if even_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Even_list.append(file_number)
                even_cache_sum += file_number
        else : 
            if odd_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Odd_list.append(file_number)
                odd_cache_sum += file_number



        file_list.append(file_number)
    
    Data_thread = threading.Thread(target=connect_to_data_server, args=(Data_server[0],Data_server[1],Data_server[2],Data_request_list))
    Even_Cache_thread = threading.Thread(target=connect_to_cache_server, args=(Even_Cache_server[0],Even_Cache_server[1],Even_Cache_server[2],Even_list))
    Odd_Cache_thread = threading.Thread(target=connect_to_cache_server, args=(Odd_Cache_server[0],Odd_Cache_server[1],Odd_Cache_server[2],Odd_list))
    
    Data_thread.start()
    Even_Cache_thread.start()
    Odd_Cache_thread.start()

    Data_thread.join()
    Odd_Cache_thread.join()
    Even_Cache_thread.join()

if __name__ == "__main__":
    client()
