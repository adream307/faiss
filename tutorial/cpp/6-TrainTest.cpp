#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/utils/random.h>
#include <memory>
#include <iostream>
#include <random>

uint32_t FloatToBaised(float x) {
    uint32_t mask = 1U << 31;
    uint32_t u = *((uint32_t *) &x);
    if (u & mask) u = ~u + 1;
    else u |= mask;
    return u;
}

bool FloatEqual(float x1, float x2) {
    uint32_t u1 = FloatToBaised(x1);
    uint32_t u2 = FloatToBaised(x2);
    int ulp = 2;
    if (u1 > u2) return (u1 - u2) < ulp;
    else return (u2 - u1) < ulp;
}


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

    //========= sample data ===========
    auto line_size = sizeof(float) * d;
    std::vector<int> perm(nb);
    faiss::rand_perm(perm.data(), perm.size(), index.cp.seed);
    int sample_size = nlist * index.cp.max_points_per_centroid;
    std::unique_ptr<float[]> sample_data(new float[d * sample_size]);
    for (int i = 0; i < sample_size; i++) {
        memcpy(sample_data.get() + i * d, xb.get() + perm[i] * d, line_size);
    }

    //============ init centroids ========
    perm.resize(sample_size);
    faiss::rand_perm(perm.data(), perm.size(), index.cp.seed + 1);
    std::vector<float> centroids(nlist * d);
    for (int i = 0; i < nlist; i++) {
        memcpy(centroids.data() + i * d, sample_data.get() + perm[i] * d, line_size);
    }

    //============= local train ===========
    std::vector<faiss::Index::idx_t> assign(sample_size);
    std::vector<float> dist(sample_size);
    faiss::IndexFlatL2 train(d);
    for (int iter = 0; iter < index.cp.niter; iter++) {
        train.reset();
        train.add(nlist, centroids.data());
        train.search(sample_size, sample_data.get(), 1, dist.data(), assign.data());

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
            float normal = 1.0 / hassign[i];
            for (int j = 0; j < d; j++) {
                centroids[i * d + j] *= normal;
            }
        }
    }
    //======================
    std::cout << "local train :" << std::endl;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
            std::cout << centroids[d * i + j] << " ";
        }
        std::cout << std::endl;
    }

    for (int i = 0; i < centroids.size(); i++) {
        if (FloatEqual(quantizer.xb[i], centroids[i]) == false) {
            std::printf("local train failed, index = %d, train = %f, local = %f\n", i, quantizer.xb[i], centroids[i]);
        }
    }

    return 0;
}
