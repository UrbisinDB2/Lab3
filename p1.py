import struct, os
from math import floor

class Record:
    FORMAT = 'i40sif15s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id: int, nombre: str, cantidad: int, precio: float, fecha: str):
        self.id = id
        self.nombre = nombre
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha

    def pack(self) -> bytes:
        return struct.pack(self.FORMAT,
        self.id, self.nombre[:30].ljust(20).encode(),
        self.cantidad, self.precio, self.fecha[:15].ljust(15).encode()
        )

    @staticmethod
    def unpack(data: bytes):
        id, nombre, cantidad, precio, fecha = struct.unpack(Record.FORMAT, data)
        return Record(id, nombre.decode().rstrip(), cantidad, precio, fecha.decode().rstrip())

    def __str__(self):
        return (str(self.id) + " | " + self.nombre + " | " + str(self.cantidad) + " | " +
                str(self.precio) + " | " + str(self.fecha))

BUFFER_SIZE = 1024
BLOCK_FACTOR = 5
INDEX_FACTOR = 5

class Page:
    FORMAT_HEADER = 'ii' #size, next_page
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records = [], next_page = -1):
        self.records = records
        self.next_page = next_page

    def pack(self) -> bytes:
        # 1- empaquetar el size y el next_page
        header_data = struct.pack(self.FORMAT_HEADER, len(self.records), self.next_page)
        record_data = b''
        for record in self.records:
            record_data += record.pack()
        i = len(self.records)
        while i < BLOCK_FACTOR:
            record_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(Page.FORMAT_HEADER, data[:Page.SIZE_HEADER])
        offset = Page.SIZE_HEADER
        records = []
        for i in range(size):
            record_data = data[offset : offset + Record.SIZE_OF_RECORD]
            record = Record.unpack(record_data)
            records.append(record)
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page)

class IndexFile:
    FORMAT_HEADER = 'i'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_INDEX = SIZE_HEADER + struct.calcsize('i') * INDEX_FACTOR + struct.calcsize('i') * (INDEX_FACTOR + 1)

    def __init__(self, file_name: str):
        self.file_name = file_name
        self.pages = []
        self.keys = []

    def getIndex(self):
        pages = []
        keys = []
        with open(self.file_name, 'rb') as file:
            size = struct.unpack('i', file.read(4))[0]

            for i in range(size):
                pi = struct.unpack('i', file.read(4))[0]
                pages.append(pi)
                ki = struct.unpack('i', file.read(4))[0]
                keys.append(ki)

        return pages, keys


    def addIndex(self, page_pos: int, key: int):
        import os
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                file.write(struct.pack(self.FORMAT_HEADER, 1))
                file.write(struct.pack('i', page_pos))
                file.write(struct.pack('i', key))
            return

    def search_position(self, record_id: int):
        pages, keys = self.getIndex()

        left, right = 0, len(keys) - 1

        while left <= right:
            mid = (left + right) // 2
            if keys[mid] < record_id:
                left = mid + 1
            else:
                right = mid - 1



        return left


    def scanALL(self):
        try:
            with open(self.file_name, 'rb') as file:
                size = struct.unpack(self.FORMAT_HEADER, file.read(self.SIZE_HEADER))[0]
                print("Index Size = ", str(size))
            pages, keys = self.getIndex()
            print("Pages: ", end='')
            for page in pages:
                print(str(page) + ", ")
            print("Keys: ", end='')
            for key in keys:
                print(str(key) + ", ")
        except FileNotFoundError:
            print("File not found")

class ISAM:
    def __init__(self, file_name):
        self.file_name = file_name

    def add(self, record: Record):
        indexf = IndexFile("index.dat")
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                new_page = Page([record])
                page_pos = file.tell()
                indexf.addIndex(page_pos, record.id)
                file.write(new_page.pack())
            return

        pos = indexf.search_position(record.id)
        print("Pos = ", pos)

        with open(self.file_name, 'r+b') as file:
            file.seek()

        # with open(self.file_name, 'r+b') as file:
        #     file.seek(0, 2)
        #     filesize = file.tell()
        #     pos_last_page = filesize - Page.SIZE_OF_PAGE
        #     file.seek(pos_last_page, 0)
        #     page = Page.unpack(file.read(Page.SIZE_OF_PAGE))
        #     # si hay espacio, agregamos el registro a la pagina
        #     if len(page.records) < BLOCK_FACTOR:
        #         page.records.append(record)
        #         file.seek(pos_last_page, 0)
        #         file.write(page.pack())
        #     else:
        #         # crear nueva pagina
        #         file.seek(0, 2)
        #         new_page = Page([record])
        #         file.write(new_page.pack())

    def scanAll(self):
        # Iterar en todas las paginas y mostrar la informacion de los registros
        with open(self.file_name, 'rb') as file:
            file.seek(0, 2)
            numPages = file.tell() // Page.SIZE_OF_PAGE
            file.seek(0, 0)
            for i in range(numPages):
                print("-- Page ", i + 1)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                for record in page.records:
                    print(record)

## Main
dataf = ISAM("data.dat")
indexf = IndexFile("index.dat")
dataf.add(Record(1, "Estabilizador de Voltaje", 25, 192.26, "2024-10-21"))
# dataf.add(Record(2, "Bascula Inteligente", 43, 1809.71, "2024-05-07"))
# dataf.add(Record(3, "Estabilizador de Voltaje", 7, 1204.21, "2024-08-21"))
indexf.scanALL()
dataf.scanAll()

