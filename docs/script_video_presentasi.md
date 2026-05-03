# Script Video Demonstrasi — Distributed Sync System

## Catatan Penting
- Gunakan Bahasa Indonesia yang jelas dan profesional.
- Jangan sebut angka fitur yang belum dibuktikan di layar.
- Angka performa yang aman disebut: `619 requests`, `0 failures`, `avg 41.9 ms`, `P95 78 ms`, `P99 85 ms`.
- Fokus demo pada fitur yang benar-benar ada di repo: `Raft`, `Lock Manager`, `Queue`, `Cache`, `Locust`, `Prometheus`, dan `Grafana`.

---

## 1. Pendahuluan dan Tujuan (1–2 menit)

**Yang tampil di layar:** wajah Anda + `README.md` atau slide judul.

> Halo, perkenalkan nama saya Aisyah Wilda Fauziah Amanda, dengan NIM 11231005. Pada video ini saya akan mempresentasikan tugas Sistem Paralel dan Terdistribusi, yaitu implementasi **Distributed Synchronization System** dari awal.
>
> Tujuan utama sistem ini adalah mengelola resource bersama agar tidak terjadi konflik antar node. Di dalam sistem ini saya membangun tiga komponen utama, yaitu **Distributed Lock Manager**, **Distributed Queue**, dan **Distributed Cache**.
>
> Selain itu, sistem ini juga menggunakan **Raft Consensus** untuk menjaga konsistensi dan toleransi terhadap kegagalan node. Pada video ini saya akan menunjukkan arsitektur sistem, demonstrasi fitur utama, pengujian performa, dan kesimpulan dari implementasi yang sudah saya buat.

---

## 2. Penjelasan Arsitektur Sistem (2–3 menit)

**Yang tampil di layar:** diagram arsitektur dari laporan atau `README.md`.

> Pertama, saya jelaskan arsitekturnya.
>
> **Lock Manager** berfungsi mengatur akses eksklusif ke resource. Analognya seperti peminjaman buku: kalau satu orang sedang memakai resource, maka user lain harus menunggu sampai resource itu dilepas. Bagian ini penting untuk memastikan **mutual exclusion**.
>
> Kedua, **Queue Manager**. Komponen ini bekerja seperti sistem antrean. Pesan yang masuk akan diproses secara terdistribusi menggunakan **consistent hashing**, sehingga beban bisa tersebar ke node yang sesuai.
>
> Ketiga, **Cache Manager**. Komponen ini menyimpan data yang sering diakses supaya respon lebih cepat. Untuk menjaga agar data antar node tetap konsisten, saya menggunakan mekanisme koherensi cache berbasis **MESI**.
>
> Lalu bagian paling penting adalah **Raft Consensus**. Raft dipakai untuk memastikan perubahan state tetap konsisten meskipun ada node yang gagal. Jadi kalau satu node mati, sistem masih bisa berjalan karena node lain bisa mengambil peran secara otomatis.
>
> Dengan desain ini, sistem saya membagi fungsi sesuai kebutuhan: **lock untuk konsistensi kuat**, **queue untuk distribusi pesan**, dan **cache untuk performa baca yang cepat**.

---

## 3. Live Demo Semua Fitur (5–7 menit)

### 3A. Start cluster Docker

**Yang tampil di layar:** terminal utama.

> Sekarang saya masuk ke demonstrasi langsung. Pertama saya jalankan seluruh node menggunakan Docker Compose.

```powershell
cd "E:\SEMESTER 6\SisTer\Tugas 3\distributed-sync-system\docker"
docker compose up -d --build
docker compose ps
```

> Dari hasil `docker compose ps`, bisa dilihat bahwa node-node sudah berjalan dan saling terhubung dalam satu cluster.

### 3B. Buka Swagger UI

**Yang tampil di layar:** browser `http://localhost:8001/docs`.

> Selanjutnya saya buka Swagger UI untuk mencoba endpoint yang tersedia. Di sini saya bisa melihat endpoint untuk lock, queue, cache, dan health check.

### 3C. Demo Lock Manager

> Pertama saya coba fitur **Lock Manager**. Saya akan mengirim request untuk mengambil lock eksklusif pada sebuah resource. Sistem merespons sukses, artinya resource tersebut sedang dikunci dan hanya bisa dipakai oleh satu pihak.

**Body JSON:**
```json
{
  "node_id": "demo1",
  "resource_id": "resource_1",
  "lock_type": "exclusive"
}
```

> Ini menunjukkan bahwa mekanisme mutual exclusion berjalan dengan benar.

### 3D. Demo Queue

> Kedua, saya coba fitur **Queue**. Saya akan mengirim sebuah pesan ke antrean.

**Body JSON:**
```json
{
  "key": "queue_1",
  "content": "halo"
}
```

> Pesan berhasil masuk ke queue. Setelah itu saya cek proses dequeue untuk memastikan pesan bisa diproses kembali dari node yang sesuai.
>
> Fitur queue ini memakai consistent hashing, jadi distribusi data bisa tetap stabil walaupun jumlah node berubah.

### 3E. Demo Cache

> Ketiga, saya coba fitur **Cache**. Saya simpan data ke cache, lalu saya ambil kembali datanya.

**Body JSON:**
```json
{
  "key": "cache_key_1",
  "value": "demo"
}
```

> Saat saya lakukan get, datanya muncul sangat cepat. Ini menunjukkan cache bekerja untuk mempercepat akses data yang sering dipakai.

### 3F. Demo failover / Raft

> Sekarang saya tunjukkan bagian fault tolerance. Saya akan mematikan salah satu node untuk melihat apakah sistem tetap berjalan.

```powershell
docker stop node1
```

> Setelah node ini dimatikan, node lain tetap bisa melayani request karena cluster masih memiliki quorum. Ini adalah salah satu manfaat Raft, yaitu sistem tetap bisa beroperasi walaupun salah satu node mengalami kegagalan.
>
> Jika saya nyalakan lagi:

```powershell
docker start node1
```

> Setelah dinyalakan kembali, node akan bergabung lagi ke cluster.

---

## 4. Performance Testing (2–3 menit)

**Yang tampil di layar:** terminal benchmark Locust.

> Setelah demonstrasi fitur utama, saya lanjut ke pengujian performa. Saya menjalankan benchmark menggunakan Locust untuk mensimulasikan 100 concurrent users selama 5 menit.

```powershell
cd "E:\SEMESTER 6\SisTer\Tugas 3\distributed-sync-system"
C:/Users/USER/AppData/Local/Programs/Python/Python310/python.exe -m locust -f .\benchmarks\locustfile.py --headless -u 100 -r 10 --run-time 5m --host http://localhost:8001 --csv .\results\final_benchmark
```

> Dari hasil benchmark, sistem menghasilkan **619 requests** dengan **0 failures**. Rata-rata latency berada di sekitar **41.9 ms**, dengan **P95 78 ms** dan **P99 85 ms**. Ini menunjukkan sistem cukup stabil saat menerima beban concurrent.
>
> Pada komponen lock, latency memang sedikit lebih tinggi karena ada proses consensus. Sementara itu, cache dan queue tetap responsif karena dirancang untuk mendukung distribusi data dan akses yang cepat.
>
> Jadi secara keseluruhan, hasil benchmark ini menunjukkan bahwa sistem berhasil menjaga keseimbangan antara konsistensi dan performa.

---

## 5. Monitoring Prometheus dan Grafana

**Yang tampil di layar:** browser `http://localhost:9090` dan `http://localhost:3000`.

> Untuk observability, saya juga menyiapkan **Prometheus** dan **Grafana**. Prometheus dipakai untuk mengumpulkan metrik, sedangkan Grafana dipakai untuk memvisualisasikannya.
>
> Di Prometheus, saya bisa melihat metrik seperti throughput, lock count, cache hit, dan queue activity. Sedangkan di Grafana, metrik tersebut ditampilkan dalam bentuk dashboard agar lebih mudah dianalisis.

**PromQL yang aman dipakai:**
```promql
rate(locks_acquired_total[1m])
rate(messages_enqueued_total[1m])
rate(messages_dequeued_total[1m])
rate(cache_hits_total[1m]) / (rate(cache_hits_total[1m]) + rate(cache_misses_total[1m])) * 100
histogram_quantile(0.95, sum(rate(lock_wait_time_seconds_bucket[1m])) by (le))
histogram_quantile(0.95, sum(rate(cache_operation_time_seconds_bucket[1m])) by (le))
```

---

## 6. Kesimpulan dan Tantangan (1–2 menit)

**Yang tampil di layar:** wajah Anda.

> Sebagai kesimpulan, implementasi **Distributed Synchronization System** ini berhasil menunjukkan bagaimana pembagian beban dan toleransi kegagalan bisa diterapkan dalam sistem terdistribusi. Semua komponen utama sudah berjalan dalam container Docker dan bisa diuji melalui API, benchmark, serta monitoring.
>
> Tantangan terbesar yang saya hadapi adalah menyusun state transition pada Raft dan memastikan setiap komponen tetap konsisten saat terjadi perubahan node. Selain itu, saya juga harus menyesuaikan antara kebutuhan konsistensi dan performa, karena sistem terdistribusi memang selalu memiliki trade-off antara latency dan reliability.
>
> Demikian presentasi dari saya mengenai Distributed Sync System. Terima kasih atas perhatiannya. Wassalamualaikum warahmatullahi wabarakatuh.

---

## Checklist Rekam Cepat
- Buka `README.md` atau diagram arsitektur.
- Start cluster Docker.
- Buka Swagger UI.
- Demo lock.
- Demo queue.
- Demo cache.
- Demo failover.
- Jalankan Locust.
- Buka Prometheus dan Grafana.
- Tutup dengan kesimpulan.

## URL yang Dibuka
- `http://localhost:8001/docs`
- `http://localhost:9090`
- `http://localhost:3000`
- `http://localhost:8001/health`
- `http://localhost:8001/queue/status`
- `http://localhost:8001/cache/get/cache_key_1`
