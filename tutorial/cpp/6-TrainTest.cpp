#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/utils/random.h>
#include <memory>
#include <iostream>
#include <random>

float distance(float *p1, float *p2, int n) {
    float dist = 0;
    for (int i = 0; i < n; i++) {
        dist += (p1[i] - p2[i]) * (p1[i] - p2[i]);
    }
    return dist;
}

int search(float *xb, int dim, int nb, float *src) {
    float dist = distance(xb, src, dim);
    int idx = 0;
    for (int i = 1; i < nb; i++) {
        float l2 = distance(xb + i * dim, src, dim);
        if (l2 < dist) {
            dist = l2;
            idx = i;
        }
    }
    return idx;
}

//int main() {
//    int d = 64;
//    int nlist = 100;
//    int nq = 1;
//    std::mt19937 rng;
//    std::unique_ptr<float[]> xb(new float[d * (nlist + nq)]);
//    for (int i = 0; i < nlist + nq; i++) {
//        double sum = 0;
//        for (int j = 0; j < d; j++) {
//            xb[d * i + j] = rand() % 1000;
//            sum += xb[d * i + j] * xb[d * i + j];
//        }
//        sum = sqrt(sum);
//        double total = 0;
//        for (int j = 0; j < d; j++) {
//            xb[d * i + j] /= sum;
//            total += xb[d * i + j] * xb[d * i + j];
//        }
//        std::cout << "i = " << i << ", total = " << total << std::endl;
//    }
//    float *xq = xb.get() + nlist * d;
//    faiss::IndexFlatL2 index(d);
//    index.add(nlist, xb.get());
//    faiss::Index::idx_t o_idx;
//    float dist;
//
//    index.search(1, xq, 1, &dist, &o_idx);
//    std::cout << "index flat : " << o_idx << std::endl;
//    std::cout << "search : " << search(xb.get(), d, nlist, xq) << std::endl;
//    return 0;
//}

int main() {
    int d = 64;
    int nb = 100000;
    int nlist = 100;

    std::mt19937 rng;

    std::unique_ptr<float[]> xb(new float[d * nb]);

    for (int i = 0; i < nb; i++) {
        double sum = 0;
        for (int j = 0; j < d; j++) {
            xb[d * i + j] = rng() % 1000;
            sum += xb[d * i + j] * xb[d * i + j];
        }
        sum = sqrt(sum);
        for (int j = 0; j < d; j++) {
            xb[d * i + j] /= sum;
        }
    }

    faiss::IndexFlatL2 quantizer(d);
    faiss::IndexIVFFlat index(&quantizer, d, nlist);
    index.train(nb, xb.get());

    std::cout << "train :" << std::endl;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
            std::cout << quantizer.xb[d * i + j] << " ";
        }
        std::cout << std::endl;
    }

    std::cout << "=====================" << std::endl;

    auto line_size = sizeof(float) * d;
    std::vector<int> perm(nb);
    faiss::rand_perm(perm.data(), perm.size(), index.cp.seed);
    int sample_size = nlist * index.cp.max_points_per_centroid;
    std::unique_ptr<float[]> sample_data(new float[d * sample_size]);
    for (int i = 0; i < sample_size; i++) {
        memcpy(sample_data.get() + i * d, xb.get() + perm[i] * d, line_size);
    }
    std::cout << "sample data:" << std::endl;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
            std::cout << sample_data[i * d + j] << " ";
        }
        std::cout << std::endl;
    }

    //=================================
    perm.resize(sample_size);
    faiss::rand_perm(perm.data(), perm.size(), index.cp.seed + 1);
    std::vector<float> centroids(nlist * d);
    for (int i = 0; i < nlist; i++) {
        memcpy(centroids.data() + i * d, sample_data.get() + perm[i] * d, line_size);
    }

    //===========================
    std::vector<int> assign(sample_size);
    for (int iter = 0; iter < index.cp.niter; iter++) {
        std::cout << "train iter = " << iter << ", centroids:" << std::endl;
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                std::cout << centroids[i * d + j] << " ";
            }
            std::cout << std::endl;
        }

        for (int i = 0; i < sample_size; i++) {
            assign[i] = search(centroids.data(), d, nlist, sample_data.get() + i * d);
        }
        std::cout << "train iter = " << iter << ", assign: ";
        for (int i = 0; i < 5; i++) std::cout << assign[i] << " ";
        std::cout << std::endl;

        memset(centroids.data(), 0, sizeof(float) * centroids.size());
        std::vector<float> hassign(nlist, 0);
        for (int i = 0; i < sample_size; i++) {
            int ci = assign[i];
            float *c = centroids.data() + ci * d;
            float *xi = sample_data.get() + i * d;
            hassign[ci] += 1;
            for (int j = 0; j < d; j++) {
                c[j] += xi[j];
            }
        }
        for (int i = 0; i < nlist; i++) {
            std::cout << hassign[i] << " ";
            float normal = 1.0 / hassign[i];
            for (int j = 0; j < d; j++) {
                centroids[i * d + j] *= normal;
            }
        }
        std::cout << std::endl;
    }
    //======================
    std::cout << "local train :" << std::endl;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
            std::cout << centroids[d * i + j] << " ";
        }
        std::cout << std::endl;
    }

    return 0;
}
