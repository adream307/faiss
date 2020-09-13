#include <iostream>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>
#include <faiss/OnDiskInvertedLists.h>
#include <vector>

int main() {
  int dim = 64;
  int nb = 100000; //database size 10W

  auto *xb = new float[dim * nb];

  for (int i = 0; i < nb; i++) {
    for (int j = 0; j < dim; j++) {
      xb[i * dim + j] = drand48();
    }
  }

  int nlist = 100;
  faiss::IndexFlatL2 quantizer(dim);
  faiss::IndexIVFFlat index(&quantizer, dim, nlist, faiss::MetricType::METRIC_L2);
  index.train(nb, xb);

  faiss::OnDiskInvertedLists invlist(index.nlist, index.code_size, "/tmp/exp3.ondisk");
//  std::vector<uint8_t > code(index.code_size);
//  invlist.add_entry(0, 0, code.data());

  index.replace_invlists(&invlist);

  std::cout << "nlist " << index.nlist << " code size " << index.code_size << std::endl;

  faiss::write_index(&index, "/tmp/exp3.ivf");
  std::cout << "write index file success" << std::endl;

  auto *readidx = faiss::read_index("/tmp/exp3.ivf");
  std::vector<uint8_t > code(index.code_size);
  invlist.add_entry(0, 0, code.data());

  delete readidx;
  delete[] xb;

  return 0;
}
