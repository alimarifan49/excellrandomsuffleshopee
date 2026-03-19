import pandas as pd
import random
import os
import tkinter as tk
from tkinter import filedialog, messagebox

# ===== CONFIG =====
LINK_COLUMNS = ["link2", "link3", "link4", "link5", "link6"]

# ===== FUNCTION PROCESS =====
def process_file(file_path):
    try:
        # FIX dtype
        df = pd.read_excel(file_path, dtype=str)
        df = df.fillna("")

        # ===== VALIDASI =====
        required_cols = ["link1", "nama produk", "status"]
        for col in required_cols:
            if col not in df.columns:
                messagebox.showerror("Error", f"Kolom '{col}' tidak ditemukan!")
                return

        # Normalisasi
        df["nama produk"] = df["nama produk"].str.strip()
        df["link1"] = df["link1"].str.strip()
        df["status"] = df["status"].str.strip().str.lower()

        # Pastikan kolom link2-6 ada
        for col in LINK_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        # ===== PROCESS =====
        for idx, row in df.iterrows():

            # 🚨 SKIP jika bukan pending
            if row["status"] != "pending":
                continue

            kategori = row["nama produk"]

            # Ambil kategori sama
            same_category = df[df["nama produk"] == kategori]

            # Buang diri sendiri
            same_category = same_category.drop(idx)

            # Ambil link valid
            links = same_category["link1"].tolist()
            links = [l for l in links if l]

            # Shuffle
            random.shuffle(links)

            # Ambil max 5 tanpa duplikat
            selected_links = links[:len(LINK_COLUMNS)]

            # Isi kolom
            for i, col in enumerate(LINK_COLUMNS):
                if i < len(selected_links):
                    df.at[idx, col] = selected_links[i]
                else:
                    df.at[idx, col] = ""

        # ===== SAVE =====
        folder = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        output_path = os.path.join(folder, "output_" + filename)

        df.to_excel(output_path, index=False)

        messagebox.showinfo("Sukses", f"File berhasil dibuat:\n{output_path}")

    except Exception as e:
        messagebox.showerror("Error", str(e))


# ===== GUI =====
def browse_file():
    file_path = filedialog.askopenfilename(
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if file_path:
        entry_file.delete(0, tk.END)
        entry_file.insert(0, file_path)


def run_process():
    file_path = entry_file.get()
    if not file_path:
        messagebox.showwarning("Warning", "Pilih file Excel dulu!")
        return

    process_file(file_path)


# ===== APP =====
root = tk.Tk()
root.title("Random Link Shopee Tool")
root.geometry("500x200")

label = tk.Label(root, text="Pilih File Excel:")
label.pack(pady=10)

entry_file = tk.Entry(root, width=60)
entry_file.pack(pady=5)

btn_browse = tk.Button(root, text="Browse", command=browse_file)
btn_browse.pack(pady=5)

btn_process = tk.Button(root, text="Proses", command=run_process, bg="green", fg="white")
btn_process.pack(pady=15)

root.mainloop()