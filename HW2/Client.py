import socket
import threading
import time
import random
import pickle

client_id = None
log_file = None

file_list = [] #다운받을 리스트
file_list_lock = threading.Lock()

def send_result(client_socket, result):
    data_to_send = pickle.dumps(result)
    client_socket.sendall(data_to_send)

# 다운로드할 파일을 요청하는 함수
def request_file(client_socket,request_list,server_name):
    flag_rq = "request"
    try:
        while True:
            with file_list_lock:
                if not file_list:
                    print("All task complete")
                    send_result(client_socket,"complete")
                    break
                file_number = file_list[0]
            if not request_list:
                continue
            try:
                if file_number == request_list[0]:
                    with file_list_lock:
                        file_list.pop(0)
                    print(f"Request file{file_number} to {server_name}")
                    send_result(client_socket,file_number,flag_rq)
                    request_list.pop(0)
                else:
                    continue
            except Exception as e:
                print(f"Failed to send request file{file_number} to {server_name}")
    except Exception as e:
        print(f"Failed to send request to {server_name}: {e}")


# 서버들로부터 정보를 받는 함수
def receive_file(client_socket,server_name):
    while True: 
        try:
            receive_data = client_socket.recv(1024)
            try:
                receive_result = pickle.loads(receive_data)
                print(f"Received data from {server_name}: {receive_result}")
            except pickle.UnpicklingError as e:
                print(f"Error unpickling data : {e}")
                break
            # 받은 데이터 정리할 코드 필요 

        except socket.timeout:
            print(f"Connection timed out ")
        except Exception as e:
            print("Error receiving data because {e}")
    return 0

def connect_to_data_server(server_address, server_port, server_name, request_list):
    global client_id
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(10)
        client_socket.connect((server_address, server_port))

        client_id = pickle.loads(client_socket.recv(1024))

        print(f"Connected to {server_name} on port {server_port}")
        time.sleep(10)
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
        time.sleep(10)
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
    
    target = 7412
    average = 0
    sum = 0
    count = 0

    for _ in range(1000):
        file_number = random.randint(1,10000)
        if file_number > target:
            Data_request_list.append(file_number)
        else:
            if file_number % 2 == 0:
                Even_list.append(file_number)
            else : 
                Odd_list.append(file_number)

        # target의 값을 적응형으로 변경
        sum += file_number
        count += 1
        average = sum / count

        if average * 1.26 > target:
            target += 1
        elif average * 1.26 < target:
            target -= 1

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
