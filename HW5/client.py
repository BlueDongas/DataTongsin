import socket
import threading
import json
import time
import queue
import random

connected_peers = [] # 현재 연결된 피어 소켓 리스트
# connected_client_id = []
peer_to_connect = queue.Queue() # 연결해야하는 피어 소켓 큐
client_id = -1
is_exit = False
message_id = 0
message_log = queue.Queue()

client_clock = 0
time_lock = threading.Lock()

def increment_clock():
    global client_clock
    while True:
        with time_lock:
            client_clock +=1
        time.sleep(1)

def get_clock():
    global client_clock

    clock = client_clock
    return clock

def log_message(log):
    clock = get_clock()
    """로그 메시지를 출력하는 함수 (입력 상태를 보존)"""
    print(f"\r{log}")  # 기존 입력 줄 덮어쓰기
    print(f"메시지를 입력하세요 : ", end="", flush=True)  # 입력 프롬프트 복원

def increment_clock():
    global client_clock
    while True:
        with time_lock:
            client_clock +=1
        time.sleep(1)

def get_clock():
    global client_clock

    clock = client_clock
    return clock

def start_peer_listener(port):
    global is_exit, connected_peers, client_id
    """
    다른 클라이언트의 연결 요청을 리슨
    """
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('0.0.0.0', port))
    listener.listen(5)
    print(f"리슨 소켓 시작 (포트: {port})")

    while not is_exit:
        peer_socket, peer_address = listener.accept()
        
        received_data = json.loads(peer_socket.recv(4096).decode('utf-8'))
        peer_id = received_data["client_id"]

        log_message(f"Clock {client_clock} : 피어 ID : [{peer_id}] {peer_address}와 연결됨")
        connected_peers.append({"socket": peer_socket, "id": peer_id})

        if len(connected_peers) > 4:
            log_message(f"연결된 클라이언트가 4개를 초과했습니다. 연결을 조정합니다.")
            handle_overflow(peer_id)

        threading.Thread(target=handle_peer, args=(peer_socket,)).start()

def handle_overflow(new_client_id):
    global connected_peers, client_clock

    # 서버에 Find 요청 보내기
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect(('127.0.0.1', 12345))
    find_message = json.dumps({"type": "FIND", "client_id": client_id, "target_id": new_client_id})
    server_socket.send(find_message.encode('utf-8'))

    # 서버에서 새로 연결할 클라이언트 정보 수신
    new_client_info = json.loads(server_socket.recv(4096).decode('utf-8'))
    server_socket.close()
    log_message(f"Clock {client_clock} : 서버로부터 받은 클라이언트 정보: {new_client_info}")

    # 기존 클라이언트 중 랜덤으로 1개 선택하여 DISCONNECT 메시지 전송
    eligible_peers = [peer for peer in connected_peers if peer["id"] != new_client_id]
    random_peer = random.choice(eligible_peers)
    disconnect_message = {
        "type": "DISCONNECT",
        "client_id": client_id,
        "data": new_client_info  # 새 클라이언트 정보
    }

    try:
        random_peer["socket"].send(json.dumps(disconnect_message).encode('utf-8'))
    except Exception as e:
        log_message(f"해제 요청 실패: {e}")
    
    # 해당 클라이언트와 연결 해제
    random_peer["socket"].close()
    connected_peers = [peer for peer in connected_peers if peer["id"] != random_peer["id"]]

def handle_peer(peer_socket):
    global is_exit, connected_peers, peer_to_connect, message_id, message_log,client_clock
    """
    다른 클라이언트로부터 받은 통신 처리
    """
    while not is_exit:
        try:
            receive_data = json.loads(peer_socket.recv(4096).decode('utf-8'))
            type = receive_data["type"]
            data = receive_data["data"]
            receive_client_id = receive_data["client_id"]
            
            if type == "DISCONNECT":
                log_message(f"Clock {client_clock} : disconnect 감지 : client {receive_client_id}")
                
                missing_peers = []
                connected_ids = [peer["id"] for peer in connected_peers]  # 현재 연결된 클라이언트들의 ID 리스트

                for peer in data:  # data는 [{ "ip": ..., "port": ..., "id": ... }]
                    if peer["id"] not in connected_ids and peer["id"] != client_id:
                        missing_peers.append(peer)
                        if len(connected_peers) + len(missing_peers) >= 4:
                            break  # 연결된 피어 + 추가할 피어가 4개가 되면 종료
                
                if missing_peers:
                    log_message(f"Clock {client_clock} : 새로 연결할 피어: {missing_peers}")
                    for missing_peer in missing_peers:
                        peer_to_connect.put(missing_peer)
                else:
                    log_message(f"Clock {client_clock} : 이미 연결이 모두 진행되어있습니다.")
                
                disconnecting_peer = peer_socket.getpeername() # DISCONNECT 메시지를 보낸 클라이언트 정보 추출
                connected_peers = [peer for peer in connected_peers if peer["id"] != receive_client_id]
                peer_socket.close()
                log_message(f"Clock {client_clock} : 피어 {disconnecting_peer['ip']}:{disconnecting_peer['port']}와의 연결이 종료되었습니다.")
                break
            elif type == "MESSAGE":
                if not data:
                    break

                receive_message_id, message_content = data

                if any(log[0] == receive_message_id for log in list(message_log.queue)): # 전달받은 메시지라면 무시
                    continue

                message_id = max(message_id, receive_message_id)  # 메시지 ID 갱신
                message_log.put((receive_message_id, message_content))

                log_message(f"Clock {client_clock} : [메시지 수신] ID: {receive_message_id}, 내용: {message_content}")

                for peer in connected_peers:
                    if peer["id"] != receive_client_id:  # 보낸 클라이언트 제외
                        try:
                            forward_data = {
                                "type": "MESSAGE",
                                "client_id": client_id,
                                "data": (receive_message_id, message_content)
                            }
                            peer["socket"].send(json.dumps(forward_data).encode('utf-8'))
                        except Exception as e:
                            log_message(f"Clock {client_clock} : [브로드캐스트 실패] {peer['id']} - {e}")
        except:
            break
        


def connect_to_peers():
    global is_exit, connected_peers, peer_to_connect,client_clock
    """
    서버에서 받은 피어 정보로 연결 시도
    """
    while not is_exit:
        try:
            peer = peer_to_connect.get()
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect((peer["ip"], peer["port"]))

            connect_message = json.dumps({"type": "CONNECT", "client_id": client_id}) #?
            peer_socket.send(connect_message.encode('utf-8'))

            log_message(f"Clock {client_clock} : 피어 {peer['ip']}:{peer['port']}와 연결됨")
            connected_peers.append({"socket": peer_socket, "id": peer["id"]})
            threading.Thread(target=handle_peer, args=(peer_socket,)).start()
        except Exception as e:
            log_message(f"Clock {client_clock} : 피어 {peer['ip']}:{peer['port']} 연결 실패: {e}")


def client_program():
    global is_exit, connected_peers, peer_to_connect, client_id, message_id, client_clock,time_lock
    # 서버에 최초 연결
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect(('127.0.0.1', 12345))
    
    connect_message = json.dumps({"type": "CONNECT"})
    server_socket.send(connect_message.encode('utf-8'))  # 연결 요청

    # 서버로부터 JSON 데이터를 수신
    response = json.loads(server_socket.recv(4096).decode('utf-8'))
    clock = response["clock"]
    
    with time_lock:
        client_clock = clock

    threading.Thread(target=increment_clock, daemon=True).start()

    print(f"Clock {client_clock} : 서버에 연결됨")
    client_id = response["client_id"]
    print(f"클라이언트 ID: {client_id}")
    listening_port = response["port"]
    print(f"서버가 할당한 포트: {listening_port}")
    peer_list = response["peers"]
    print(f"연결 가능한 피어: {peer_list}")

    server_socket.close()
    print(f"Clock {client_clock} : 서버와 연결 종료")
    
    # 리슨 소켓 시작
    threading.Thread(target=start_peer_listener, args=(listening_port,)).start()
    threading.Thread(target=connect_to_peers).start()

    time.sleep(1)

    for peer in peer_list:
        peer_to_connect.put(peer)

    # 메시지 입력 및 종료 처리
    while True:
        
        message = input(f"메시지를 입력하세요 : ")
        if not message:
            print(f"Clock {client_clock} : 빈 메시지는 보낼 수 없습니다.")
            continue
        if message.lower() == "exit":
            # 서버에 다시 연결하여 종료 요청
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect(('127.0.0.1', 12345))
            disconnect_message = json.dumps({"type": "DISCONNECT", "client_id": client_id})
            server_socket.send(disconnect_message.encode('utf-8'))

            # 서버에서 연결된 다른 클라이언트 정보 수신
            remaining_clients = json.loads(server_socket.recv(4096).decode('utf-8'))
            print(f"Clock {client_clock} : 서버로부터 받은 종료 후 연결할 클라이언트 정보: {remaining_clients}")
            server_socket.close()

            response_data = {
                "type": "DISCONNECT",
                "client_id": client_id,
                "data": remaining_clients
            }

            # 연결된 피어들에게 새로운 클라이언트 정보 전달
            for peer in connected_peers:  # connected_peers의 각 항목은 딕셔너리
                peer_socket = peer["socket"]
                try:
                    peer_socket.send(json.dumps(response_data).encode('utf-8'))
                except:
                    pass
                peer_socket.close()  # 기존 연결 종료

            print("Clock {client_clock} : 모든 연결 종료. 프로그램 종료.")
            is_exit = True
            return  # 프로그램 종료

        elif message.lower() == "connection":
            connected_ids = [peer["id"] for peer in connected_peers]  # 현재 연결된 클라이언트들의 ID 리스트
            print(f"연결된 클라이언트 정보 : {connected_ids}")

        else:
            message_id += 1

            # 메시지 브로드캐스트
            for peer in connected_peers:  # connected_peers의 각 항목은 딕셔너리
                peer_socket = peer["socket"]
                try:
                    send_data = {
                    "type": "MESSAGE",
                    "client_id": client_id,
                    "data": (message_id, message)
                    }
                    peer_socket.send(json.dumps(send_data).encode('utf-8'))
                except Exception as e:
                    log_message(f"Clock {client_clock} : 메시지 전송 실패: {e}")


if __name__ == "__main__":
    client_program()
