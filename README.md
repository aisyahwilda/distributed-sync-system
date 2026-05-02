# Distributed Synchronization System

## Identitas

* Nama: Aisyah Wilda Fauziah Amanda
* NIM: 11231005
* Mata Kuliah: Sistem Paralel dan Terdistribusi

---

## Deskripsi

Tugas ini merupakan implementasi **Distributed Synchronization System** yang mensimulasikan komunikasi dan sinkronisasi antar beberapa node dalam sistem terdistribusi.

Sistem dikembangkan menggunakan **Python (FastAPI)** dan dijalankan menggunakan **Docker**.

Pendekatan yang digunakan meliputi:

* Mutual exclusion (lock)
* Consistent hashing (queue)
* Cache management
* Komunikasi antar node berbasis API

---

## Fitur Utama

### 1. Distributed Lock Manager

* Mengatur akses resource (mutual exclusion)
* Mendukung:

  * Exclusive lock

Endpoint:

* `POST /lock/exclusive/{node_id}`
* `POST /lock/release/{node_id}`

---

### 2. Distributed Queue System

* Menggunakan **consistent hashing**
* Mendukung:

  * enqueue
  * dequeue
* Simulasi multiple producer & consumer

Endpoint:

* `POST /queue/enqueue`
* `GET /queue/dequeue`

---

### 3. Distributed Cache

* Menggunakan pendekatan **LRU (Least Recently Used)**
* Menyimpan data sementara di tiap node

Endpoint:

* `POST /cache/put`
* `GET /cache/get`

---

## Arsitektur Sistem

Sistem terdiri dari 3 node:

* node1 → http://localhost:8001
* node2 → http://localhost:8002
* node3 → http://localhost:8003

Setiap node berjalan dalam container Docker dan terhubung dalam satu network.

---

## Cara Menjalankan

### 1. Clone Repository

```bash
git clone https://github.com/aisyahwilda/distributed-sync-system.git
cd distributed-sync-system
```

### 2. Jalankan Docker

```bash
docker-compose up --build
```

### 3. Akses API

* http://localhost:8001/docs
* http://localhost:8002/docs
* http://localhost:8003/docs

---

## Alur Sistem

### Queue Flow

1. Client mengirim request enqueue
2. Sistem melakukan hashing pada key
3. Data disimpan pada node tertentu
4. Client menerima response

### Cache Flow

1. Client request data
2. Node cek cache lokal
3. Jika ada → return value
4. Jika tidak → return null

### Lock Flow

1. Client request lock
2. Sistem cek ketersediaan resource
3. Jika tersedia → lock diberikan
4. Jika tidak → ditolak

---

## Hasil Pengujian

### Distributed Queue

* Data yang dimasukkan pada satu node tidak selalu tersedia di node lain
* Hal ini menunjukkan bahwa setiap node masih menggunakan storage lokal

### Distributed Cache

* Cache bekerja secara lokal pada masing-masing node

### Distributed Lock

* Lock berhasil mengontrol akses resource

---

## Limitasi Sistem

* Belum terdapat data replication antar node
* Sistem masih berupa simulasi distributed system
* Belum mengimplementasikan Raft Consensus secara penuh
* Cache coherence belum menggunakan MESI/MOSI/MOESI
* Queue belum memiliki persistence (masih in-memory)

---

## Analisis Sistem

Sistem ini menunjukkan bahwa:

* Tanpa replication, data tidak konsisten antar node
* Distributed queue membutuhkan koordinasi tambahan
* Cache lokal meningkatkan kecepatan akses, namun tidak menjamin konsistensi global

Trade-off yang terlihat:

* Consistency vs Availability

---

## Teknologi

* Python 3.10
* FastAPI
* Docker & Docker Compose
* Uvicorn

---

## Video Demo

Link: (tambahkan nanti)

---

## 📄 Laporan

File laporan:
report_11231005_Aisyah.pdf

---

## Kesimpulan

Sistem ini berhasil mensimulasikan konsep dasar distributed system seperti:

* multi-node architecture
* resource synchronization
* distributed data handling