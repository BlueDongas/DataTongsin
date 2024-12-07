import socket
import threading
import json
import time
import queue

connected_peers = [] # 현재 연결된 피어 소켓 리스트
connected_client_id = []
peer_to_connect = queue.Queue() # 연결해야하는 피어 소켓 큐
client_id = -1
is_exit = False

def start_peer_listener(port):
    global is_exit, connected_peers, connected_client_id
    """
    다른 클라이언트의 연결 요청을 리슨
    """
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('0.0.0.0', port))
    listener.listen(4)
    print(f"리슨 소켓 시작 (포트: {port})")

    while not is_exit:
        peer_socket, peer_address = listener.accept()
        
        # receive_client_id = json.loads(peer_socket.recv(4096).decode('utf-8'))

        print(f"피어 ID : [] {peer_address}와 연결됨")
        connected_peers.append(peer_socket)
        # connected_client_id.append(receive_client_id)
        threading.Thread(target=handle_peer, args=(peer_socket,)).start()


def handle_peer(peer_socket):
    global is_exit, connected_peers, peer_to_connect, connected_client_id
    """
    다른 클라이언트와의 연결 처리
    """
    while not is_exit:
        try:
            receive_data = json.loads(peer_socket.recv(4096).decode('utf-8'))
            type = receive_data["type"]
            # receive_client_id = receive_data["client_id"]
            data = receive_data["data"]
            
            if type == "DISCONNECT":
                print("disconnect 감지")
                connected_addresses = [sock.getpeername() for sock in connected_peers]

                missing_peer = None
                self_address = peer_socket.getsockname()  # 자신의 주소와 포트

                for peer in data: # 연결된 피어와 자신을 제외한 첫 번째 클라이언트를 찾음
                    if peer not in connected_addresses and peer != self_address:
                        missing_peer = peer
                        break  # 첫 번째 조건에 맞는 클라이언트를 찾으면 종료
                
                if missing_peer:
                    peer_to_connect.put(missing_peer)
                else:
                    print(f"이미 연결이 모두 진행되어있습니다.")
                
                disconnecting_peer = peer_socket.getpeername() # DISCONNECT 메시지를 보낸 클라이언트 정보 추출
                connected_peers = [sock for sock in connected_peers if sock.getpeername() != disconnecting_peer] # connected_peers에서 해당 소켓 제거
                # connected_client_id.remove(receive_client_id)
                peer_socket.close()
                print(f"피어 {disconnecting_peer['ip']}:{disconnecting_peer['port']}와의 연결이 종료되었습니다.")
                break
            elif type == "MESSAGE":
                if not data:
                    break
                print(f"피어 메시지: {data}")
        except:
            break
        


def connect_to_peers():
    global is_exit, connected_peers, peer_to_connect
    """
    서버에서 받은 피어 정보로 연결 시도
    """
    while not is_exit:
        try:
            peer = peer_to_connect.get()
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect((peer["ip"], peer["port"]))

            connected_peers.append(peer_socket)

            # connect_message = json.dumps({"client_id": client_id})
            # peer_socket.send(connect_message.encode('utf-8'))  # 클라이언트 id 전송

            print(f"피어 {peer['ip']}:{peer['port']}와 연결됨")
            threading.Thread(target=handle_peer, args=(peer_socket,)).start()
        except Exception as e:
            print(f"피어 {peer['ip']}:{peer['port']} 연결 실패: {e}")


def client_program():
    global is_exit, connected_peers, peer_to_connect, client_id
    # 서버에 최초 연결
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect(('127.0.0.1', 12345))
    print("서버에 연결됨")
    connect_message = json.dumps({"type": "CONNECT"})
    server_socket.send(connect_message.encode('utf-8'))  # 연결 요청

    # 서버로부터 JSON 데이터를 수신
    response = json.loads(server_socket.recv(4096).decode('utf-8'))
    # client_id = response["client_id"]
    # print(f"클라이언트 ID: {client_id}")
    listening_port = response["port"]
    print(f"서버가 할당한 포트: {listening_port}")
    peer_list = response["peers"]
    print(f"연결 가능한 피어: {peer_list}")

    server_socket.close()
    print("서버와 연결 종료")
    
    # 리슨 소켓 시작
    threading.Thread(target=start_peer_listener, args=(listening_port,)).start()
    threading.Thread(target=connect_to_peers).start()

    time.sleep(1)

    for peer in peer_list:
        peer_to_connect.put(peer)

    # 메시지 입력 및 종료 처리
    while True:
        message = input()
        if message.lower() == "exit":
            # 서버에 다시 연결하여 종료 요청
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect(('127.0.0.1', 12345))
            disconnect_message = json.dumps({"type": "DISCONNECT"})
            server_socket.send(disconnect_message.encode('utf-8'))

            # 서버에서 연결된 다른 클라이언트 정보 수신
            remaining_clients = json.loads(server_socket.recv(4096).decode('utf-8'))
            print(f"서버로부터 받은 종료 후 연결할 클라이언트 정보: {remaining_clients}")
            server_socket.close()

            response_data = {
                "type": "DISCONNECT",
                # "client_id": client_id,
                "data": remaining_clients
            }

            # 연결된 피어들에게 새로운 클라이언트 정보 전달
            for peer_socket in connected_peers:
                try:
                    peer_socket.send(json.dumps(response_data).encode('utf-8'))
                except:
                    pass
                peer_socket.close()  # 기존 연결 종료

            print("모든 연결 종료. 프로그램 종료.")
            is_exit = True
            return  # 프로그램 종료

        # 메시지 브로드캐스트
        for peer_socket in connected_peers:
            try:
                send_data = {
                "type": "MESSAGE",
                "data": message
                }
                peer_socket.send(json.dumps(send_data).encode('utf-8'))
            except Exception as e:
                print(f"메시지 전송 실패: {e}")


if __name__ == "__main__":
    client_program()
