import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import socket
import json
import threading
from client import *  # Import all functions from original client

class P2PClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("P2P File Sharing Client")
        self.root.geometry("800x600")

        # !For test only
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        # !
        # Network setup
        self.SERVER_HOST = ''
        self.SERVER_PORT = 65432
        self.CLIENT_PORT = 65435
        self.sock = None
        self.connect_to_server()
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(expand=True, fill='both')
        
        # Create publish and fetch tabs
        self.publish_frame = ttk.Frame(self.notebook)
        self.fetch_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.publish_frame, text='Publish')
        self.notebook.add(self.fetch_frame, text='Fetch')
        
        self.setup_publish_tab()
        self.setup_fetch_tab()
        
        # Start host service
        self.stop_event = threading.Event()
        self.host_service_thread = threading.Thread(
            target=start_host_service, 
            args=(self.CLIENT_PORT, './')
        )
        self.host_service_thread.daemon = True
        self.host_service_thread.start()

    def connect_to_server(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.SERVER_HOST, self.SERVER_PORT))
            peers_hostname = socket.gethostname()
            self.sock.sendall(json.dumps({
                'action': 'introduce', 
                'peers_hostname': peers_hostname, 
                'peers_port': self.CLIENT_PORT
            }).encode() + b'\n')
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to server: {e}")
            self.root.quit()

    def setup_publish_tab(self):
        # File selection
        ttk.Label(self.publish_frame, text="Select File to Publish:").pack(pady=10)
        self.file_path = tk.StringVar()
        ttk.Entry(self.publish_frame, textvariable=self.file_path, width=50).pack(pady=5)
        ttk.Button(self.publish_frame, text="Browse", command=self.browse_file).pack(pady=5)
        
        # Piece selection
        ttk.Label(self.publish_frame, text="Select pieces to publish:").pack(pady=10)
        self.piece_selection = tk.StringVar()
        ttk.Entry(self.publish_frame, textvariable=self.piece_selection, width=50).pack(pady=5)
        ttk.Button(self.publish_frame, text="Publish All", command=self.publish_all).pack(pady=5)
        ttk.Button(self.publish_frame, text="Publish Selected", command=self.publish_selected).pack(pady=5)
        
        # Results display
        self.publish_result = tk.Text(self.publish_frame, height=10, width=60)
        self.publish_result.pack(pady=10)

    def setup_fetch_tab(self):
        # File name input
        ttk.Label(self.fetch_frame, text="Enter filename to fetch:").pack(pady=10)
        self.fetch_filename = tk.StringVar()
        ttk.Entry(self.fetch_frame, textvariable=self.fetch_filename, width=50).pack(pady=5)
        ttk.Button(self.fetch_frame, text="Search", command=self.search_file).pack(pady=5)
        
        # Available pieces display
        ttk.Label(self.fetch_frame, text="Available pieces:").pack(pady=10)
        self.available_pieces = tk.Text(self.fetch_frame, height=10, width=60)
        self.available_pieces.pack(pady=5)
        
        # Piece selection for download
        ttk.Label(self.fetch_frame, text="Select pieces to download:").pack(pady=5)
        self.download_selection = tk.StringVar()
        ttk.Entry(self.fetch_frame, textvariable=self.download_selection, width=50).pack(pady=5)
        ttk.Button(self.fetch_frame, text="Download All", command=self.fetch_all).pack(pady=5)
        ttk.Button(self.fetch_frame, text="Download Selected", command=self.fetch_selected).pack(pady=5)

    def browse_file(self):
        filename = filedialog.askopenfilename()
        if filename:
            self.file_path.set(filename)
            piece_size = 1048576  # 512KB
            self.current_file_size = os.path.getsize(filename)
            self.pieces = split_file_into_pieces(filename, piece_size)
            self.piece_hashes = create_pieces_string(self.pieces)
            self.piece_size = piece_size
            
            # Updated piece info display
            hostname = socket.gethostname()
            piece_info = f"File split into {len(self.pieces)} pieces:\n"
            piece_info += f"Host: {hostname} (Port: {self.CLIENT_PORT})\n\n"
            for i, (piece, hash_) in enumerate(zip(self.pieces, self.piece_hashes), 1):
                piece_info += f"Piece {i}: {hash_}\n"
            self.publish_result.delete(1.0, tk.END)
            self.publish_result.insert(tk.END, piece_info)

    def publish_selected(self):
        if not hasattr(self, 'pieces'):
            messagebox.showerror("Error", "Please select a file first")
            return
            
        selected = self.piece_selection.get().strip()
        if not selected:
            messagebox.showerror("Error", "Please enter piece numbers")
            return
            
        try:
            selected_pieces = [int(x) for x in selected.split()]
            file_name = os.path.basename(self.file_path.get())
            piece_hashes = [self.piece_hashes[i-1] for i in selected_pieces]
            
            publish_piece_file(
                self.sock,
                self.CLIENT_PORT,
                file_name,
                self.current_file_size,
                piece_hashes,
                self.piece_size,
                selected_pieces
            )
            messagebox.showinfo("Success", "Pieces published successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to publish pieces: {e}")

    def publish_all(self):
        if not hasattr(self, 'pieces'):
            messagebox.showerror("Error", "Please select a file first")
            return
            
        try:
            file_name = os.path.basename(self.file_path.get())
            all_pieces = list(range(1, len(self.pieces) + 1))
            
            publish_piece_file(
                self.sock,
                self.CLIENT_PORT,
                file_name,
                self.current_file_size,
                self.piece_hashes,
                self.piece_size,
                all_pieces
            )
            messagebox.showinfo("Success", "All pieces published successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to publish pieces: {e}")

    def search_file(self):
        filename = self.fetch_filename.get().strip()
        if not filename:
            messagebox.showerror("Error", "Please enter a filename")
            return
            
        pieces = check_local_piece_files(filename)
        pieces_hash = [] if not pieces else create_pieces_string(pieces)
        num_order_in_file = [] if not pieces else [item.split("_")[-1][5:] for item in pieces]
        
        command = {
            "action": "fetch",
            "peers_port": self.CLIENT_PORT,
            "peers_hostname": socket.gethostname(),
            "file_name": filename,
            "piece_hash": pieces_hash,
            "num_order_in_file": num_order_in_file,
        }
        
        self.sock.sendall(json.dumps(command).encode() + b'\n')
        response = json.loads(self.sock.recv(4096).decode())
        
        if 'peers_info' in response:
            self.current_peers_info = response['peers_info']
            info_text = "Available pieces:\n"
            for peer_info in self.current_peers_info:
                info_text += f"Piece {peer_info['num_order_in_file']}: {peer_info['peers_hostname']} (Port: {peer_info['peers_port']})\n"
            self.available_pieces.delete(1.0, tk.END)
            self.available_pieces.insert(tk.END, info_text)
        else:
            messagebox.showinfo("Not Found", "No peers have this file")

    def fetch_selected(self):
        if not hasattr(self, 'current_peers_info'):
            messagebox.showerror("Error", "Please search for a file first")
            return
            
        selected = self.download_selection.get().strip()
        if not selected:
            messagebox.showerror("Error", "Please enter piece numbers")
            return
            
        try:
            selected_pieces = selected.split()
            download_threads = []
            
            for piece_num in selected_pieces:
                peer_info = next(
                    (p for p in self.current_peers_info if p['num_order_in_file'] == piece_num),
                    None
                )
                if peer_info:
                    downloader = PieceDownloader(
                        peer_info['peers_ip'],
                        peer_info['peers_port'],
                        peer_info['file_name'],
                        peer_info['piece_hash'],
                        peer_info['num_order_in_file']
                    )
                    download_threads.append(downloader)
                    downloader.start()
            
            # Wait for downloads to complete
            for thread in download_threads:
                thread.join()
                
            messagebox.showinfo("Success", "Selected pieces downloaded successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to download pieces: {e}")

    def fetch_all(self):
        if not hasattr(self, 'current_peers_info'):
            messagebox.showerror("Error", "Please search for a file first")
            return
            
        try:
            download_threads = []
            for peer_info in self.current_peers_info:
                downloader = PieceDownloader(
                    peer_info['peers_ip'],
                    peer_info['peers_port'],
                    peer_info['file_name'],
                    peer_info['piece_hash'],
                    peer_info['num_order_in_file']
                )
                download_threads.append(downloader)
                downloader.start()
            
            # Wait for downloads to complete
            for thread in download_threads:
                thread.join()
                
            # Check if we have all pieces and merge
            filename = self.fetch_filename.get().strip()
            if pieces := check_local_piece_files(filename):
                if len(pieces) == len(self.current_peers_info):
                    merge_pieces_into_file(pieces, filename)
                    messagebox.showinfo("Success", f"File {filename} downloaded and assembled successfully")
                else:
                    messagebox.showinfo("Partial Download", "Not all pieces were downloaded successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to download pieces: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = P2PClientGUI(root)
    root.mainloop()
