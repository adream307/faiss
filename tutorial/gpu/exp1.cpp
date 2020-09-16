#include <random>
#include <cassert>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/StandardGpuResources.h>

int main() {
    int d = 64;                            // dimension
    int nb = 100000;                       // database size
    int nq = 10000;                        // nb of queries

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    float *xb = new float[d * nb];
    float *xq = new float[d * nq];

    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < d; j++)
            xb[d * i + j] = distrib(rng);
        xb[d * i] += i / 1000.;
    }

    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < d; j++)
            xq[d * i + j] = distrib(rng);
        xq[d * i] += i / 1000.;
    }

    int k = 4;
    int nlist = 100;

    faiss::gpu::StandardGpuResources res;
    faiss::gpu::GpuIndexIVFFlat index_ivf(&res, d, nlist, faiss::METRIC_L2);

    assert(!index_ivf.is_trained);
    index_ivf.train(nb,xb);

    printf("is_trained = %s\n", index_ivf.is_trained ? "true" : "false");
    printf("ntotal = %ld\n", index_ivf.ntotal);

    {       // search xq
        long *I = new long[k * nq];
        float *D = new float[k * nq];

        index_ivf.search(nq, xq, k, D, I);

        // print results
        printf("I (5 first results)=\n");
        for(int i = 0; i < 5; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }

        printf("I (5 last results)=\n");
        for(int i = nq - 5; i < nq; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }

        delete [] I;
        delete [] D;
    }


    delete [] xb;
    delete [] xq;

    return 0;


}
