
from mpi4py import MPI


nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/data-big.txt', MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)

blocksize = int(size/nprocs)
block_start = blocksize*myid
buf=bytearray(100)
if myid==nprocs-1:
    block_end=size
else:
    block_end=block_start+blocksize-1;
    
if myid ==0:
    fh.Seek(block_start)
    fh.Iread(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    block_start+=offset
    


if myid !=(nprocs-1):    
    fh.Seek(block_end)
    fh.Iread(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    block_end+=offset