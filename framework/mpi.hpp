
#ifndef _ARES_MPI_HPP_
#define _ARES_MPI_HPP_

#include "bytes.hpp"
#include "cmd.hpp"

#include <cstring>

namespace ares_impl {

#include "mpi.h"

	class mpi_controller {
		static constexpr MPI_Comm WORLD = MPI_COMM_WORLD;
		static constexpr int MASTER_ID = 0;

		int _id;
		size_t _size;

	public:
		mpi_controller() {
			MPI_Comm_rank(MPI_COMM_WORLD, &_id);
			int tmp;
			MPI_Comm_size(MPI_COMM_WORLD, &tmp);
			_size = tmp;
		}

		int id() const { return _id; }
		size_t size() const { return _size; }
		int master() const { return MASTER_ID; }
		bool is_m() const { return id() == master(); }

		void bcast(command &head) {
			MPI_Bcast(&head, (int)sizeof(head), MPI_BYTE, master(), WORLD);
		}

		void bcast(byte_array &data, size_t len) {
			byte *buf = new byte[len];
			if ( is_m() ) {
				memcpy(buf, data.data(), len);
			}
			MPI_Bcast(buf, (int)len, MPI_BYTE, master(), WORLD);
			if ( !is_m() ) {
				data.reserve(len);
				data.write(buf, len);
			}
		}

		void scatter(const byte_array send[], byte_array &recv) {
			int sendcounts[size()], sdispls[size()];
			size_t stotal = 0;

			for (size_t k = 0; k < size() && send != nullptr; ++k) {
				sdispls[k] = (int)stotal;
				sendcounts[k] = (int)send[k].size();
				stotal += send[k].size();
			}

			int recvcount;
			MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, master(), WORLD);

			byte *sendbuf = new byte[stotal];
			byte *recvbuf = new byte[recvcount];
			for (size_t k = 0; k < size() && send != nullptr; ++k) {
				memcpy(sendbuf + sdispls[k], send[k].data(), sendcounts[k]);
			}
			MPI_Scatterv(sendbuf, sendcounts, sdispls, MPI_BYTE,
					recvbuf, recvcount, MPI_BYTE, master(), WORLD);

			recv.reserve(recvcount);
			recv.write(recvbuf, recvcount);

			delete [] recvbuf;
			delete [] sendbuf;
		}

		void alltoall(const byte_array send[], byte_array recv[]) {
			size_t sendlen = 0, recvlen = 0;
			int sendcounts[size()], sdispls[size()];
			int recvcounts[size()], rdispls[size()];

			for (size_t k = 0; k < size(); ++k) {
				sdispls[k] = (int)sendlen;
				sendcounts[k] = (int)send[k].size();
				sendlen += send[k].size();
			}
			MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, WORLD);

			for (size_t k = 0; k < size(); ++k) {
				rdispls[k] = (int)recvlen;
				recvlen += recvcounts[k];
			}

			byte *sendbuf = new byte [sendlen];
			byte *recvbuf = new byte [recvlen];
			for (size_t k = 0; k < size(); ++k) {
				memcpy(sendbuf + sdispls[k], send[k].data(), sendcounts[k]);
			}
			MPI_Alltoallv(sendbuf, sendcounts, sdispls, MPI_BYTE,
					recvbuf, recvcounts, rdispls, MPI_BYTE, WORLD);
			for (size_t k = 0; k < size(); ++k) {
				recv[k].reserve(recvcounts[k]);
				recv[k].write(recvbuf + rdispls[k], recvcounts[k]);
			}
			delete [] recvbuf;
			delete [] sendbuf;
		}

		void gather(const byte_array &send, byte_array recv[]) {
			int sendlen = (int)send.size();
			int recvcounts[size()];
			MPI_Gather(&sendlen, 1, MPI_INT, recvcounts, 1, MPI_INT, master(), WORLD);

			int rdispls[size()];
			size_t rtotal = 0;
			for (size_t k = 0; k < size() && recv != nullptr; ++k) {
				rdispls[k] = (int)rtotal;
				rtotal += recvcounts[k];
			}

			byte *recvbuf = new byte[rtotal];
			MPI_Gatherv((byte *)send.data(), (int)send.size(), MPI_BYTE,
					recvbuf, recvcounts, rdispls, MPI_BYTE, master(), WORLD);

			for (size_t k = 0; k < size() && recv != nullptr; ++k) {
				recv[k].reserve(recvcounts[k]);
				recv[k].write(recvbuf + rdispls[k], recvcounts[k]);
			}
			delete [] recvbuf;
		}
	};
}

#endif // _ARES_MPI_HPP_
