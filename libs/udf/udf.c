double ArraySum(double numbers[], int size);

double ArraySum(double numbers[], int size) {
    double sum = 0;
    for (int i = 0; i < size; i++) {
        sum = sum + numbers[i];
    }
    return sum;
}
