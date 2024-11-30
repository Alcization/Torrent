import logging
import socket
import threading
import json
import sys
import mysql.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Establish a connection to the PostgreSQL database
mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "",
  database = "peers"
)
mycursor = mydb.cursor()


def log_event(message):
    logging.info(message)

def update_client_info(peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash,piece_size,num_order_in_file):
    # First, delete any existing entries with the same piece_hash

    for i in range(len(num_order_in_file)):
        # Check if piece hash already exists
        mycursor.execute(
            "DELETE FROM peers WHERE piece_hash = %s",
            (piece_hash[i],)
        )

    
    # Then insert the new entries
    for i in range(len(num_order_in_file)):
        mycursor.execute(
            "INSERT INTO peers (peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash,piece_size,num_order_in_file) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (peers_ip,peers_port,peers_hostname,file_name,file_size,piece_hash[i],piece_size,num_order_in_file[i])
        )
    mydb.commit()

active_connections = {}  
host_files = {}

def client_handler(mydb, addr):
    try:

        while True:
            data = mydb.recv(16384).decode()
            # log_event(f"Received data from {addr}: {data}")
            if not data:
                break

            try:
                command = json.loads(data)
            except json.JSONDecodeError:
                log_event(f"Invalid JSON received from {addr}: {data}")
                continue

            peers_ip = addr[0]
            peers_port = command['peers_port']
            peers_hostname = command['peers_hostname']
            file_name = command['file_name'] if 'file_name' in command else ""
            file_size = command['file_size'] if 'file_size' in command else ""
            piece_hash = command['piece_hash'] if 'piece_hash' in command else ""
            piece_size = command['piece_size'] if 'piece_size' in command else ""
            num_order_in_file = command['num_order_in_file'] if 'num_order_in_file' in command else ""

            if command.get('action') == 'introduce':
                client_peers_hostname = command.get('peers_hostname')
                active_connections[client_peers_hostname] = mydb
                log_event(f"Connection established with {client_peers_hostname}/{peers_ip}:{peers_port})")

            elif command['action'] == 'publish':
                # peers_ip,peers_port,peers_hostname,file_name,piece_hash
                log_event(f"Updating client info in database for hostname: {peers_hostname}/{peers_ip}:{peers_port}")
                update_client_info(peers_ip,peers_port, peers_hostname,file_name,file_size, piece_hash,piece_size,num_order_in_file)  # addr[0] is the IP address
                log_event(f"Database update complete for hostname: {peers_hostname}/{peers_ip}:{peers_port}")
                mydb.sendall("File list updated successfully.".encode())

            elif command['action'] == 'fetch':
                if num_order_in_file and piece_hash:
                    num_order_in_file_tuple = tuple(num_order_in_file)
                    piece_hash_tuple = tuple(piece_hash)

                    num_order_in_file_str = ','.join(['%s'] * len(num_order_in_file_tuple))
                    piece_hash_str = ','.join(['%s'] * len(piece_hash_tuple))

                    query = f"""
                        SELECT * FROM peers 
                        WHERE file_name = %s 
                        AND num_order_in_file NOT IN ({num_order_in_file_str}) 
                        AND piece_hash NOT IN ({piece_hash_str})
                        ORDER BY num_order_in_file DESC
                    """
                    params = (file_name, *num_order_in_file_tuple, *piece_hash_tuple)
                else:
                    query = """
                        SELECT * FROM peers 
                        WHERE file_name = %s
                        ORDER BY num_order_in_file DESC
                    """
                    params = (file_name,)

                mycursor.execute(query, params)
                results = mycursor.fetchall()
                if results:
                    peers_info = [{'peers_ip': peers_ip, 'peers_port': peers_port, 'peers_hostname': peers_hostname, 'file_name': file_name, 'file_size': file_size, 'piece_hash': piece_hash, 'piece_size': piece_size, 'num_order_in_file': num_order_in_file} for peers_ip, peers_port, peers_hostname, file_name, file_size, piece_hash, piece_size, num_order_in_file in results if peers_hostname in active_connections]
                    mydb.sendall(json.dumps({'peers_info': peers_info}).encode())
                else:
                    mydb.sendall(json.dumps({'error': 'File not available'}).encode())

            elif command['action'] == 'file_list':
                files = command['files']
                print(f"List of files : {files}")

    except Exception as e:
        logging.exception(f"An error occurred while handling client {addr}: {e}")
    finally:
        if client_peers_hostname:
            del active_connections[client_peers_hostname]  
        mydb.close()
        log_event(f"Connection with {addr} has been closed.")

def request_file_list_from_client(peers_hostname):
    if peers_hostname in active_connections:
        mydb = active_connections[peers_hostname]
        print(active_connections[peers_hostname])
        ip_address, _ = mydb.getpeername()
        # print(ip_address)
        peer_port = 65433  
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.connect((ip_address, peer_port))
        request = {'action': 'request_file_list'}
        peer_sock.sendall(json.dumps(request).encode() + b'\n')
        response = json.loads(peer_sock.recv(16384).decode())
        peer_sock.close()
        if 'files' in response:
            return response['files']
        else:
            return "Error: No file list in response"
    else:
        return "Error: Client not connected"

def discover_files(peers_hostname):
    # Connect to the client and request the file list
    files = request_file_list_from_client(peers_hostname)
    print(f"Files on {peers_hostname}: {files}")

def ping_host(peers_hostname):
    mycursor.execute("SELECT address FROM client_files WHERE hostname = %s", (peers_hostname,))
    results = mycursor.fetchone()  
    ip_address = results[0]
    print(ip_address)
    if ip_address:
        peer_port = 65433
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.connect((ip_address, peer_port))
        request = {'action': 'ping'}
        peer_sock.sendall(json.dumps(request).encode() + b'\n')
        response = peer_sock.recv(16384).decode()
        peer_sock.close()
        if response:
            print(f"{peers_hostname} is online!")
        else:
            print(f"{peers_hostname} is offline!")
    else:
        print("There is no host with that name")


def server_command_shell():
    while True:
        cmd_input = input("Server command: ")
        cmd_parts = cmd_input.split()
        if cmd_parts:
            action = cmd_parts[0]
            if action == "discover" and len(cmd_parts) == 2:
                hostname = cmd_parts[1]
                thread = threading.Thread(target=discover_files, args=(hostname,))
                thread.start()
            elif action == "ping" and len(cmd_parts) == 2:
                hostname = cmd_parts[1]
                thread = threading.Thread(target=ping_host, args=(hostname,))
                thread.start()
            elif action == "exit":
                break
            else:
                print("Unknown command or incorrect usage.")

def start_server(host='0.0.0.0', port=65432):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    log_event("Server started and is listening for connections.")

    try:
        while True:
            mydb, addr = server_socket.accept()
            # host = server_socket.getsockname()
            # log_event(f"Accepted connection from {addr}, hostname is {host}")
            thread = threading.Thread(target=client_handler, args=(mydb, addr))
            thread.start()
            log_event(f"Active connections: {threading.active_count() - 1}")
    except KeyboardInterrupt:
        log_event("Server shutdown requested.")
    finally:
        server_socket.close()
        mycursor.close()
        mydb.close()

if __name__ == "__main__":
    # !For test only
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    # !
    SERVER_HOST = ''
    SERVER_PORT = 65432
    #SERVER_HOST='0.0.0.0'
    # Start server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    # Start the server command shell in the main thread
    server_command_shell()

    # Signal the server to shutdown
    print("Server shutdown requested.")
    
    sys.exit(0)