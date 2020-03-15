double ArraySum(double numbers[], int size);

double ArraySum(double numbers[], int size) {
    double sum = 0;
    for (int i = 0; i < size; i++) {
        sum += numbers[i];
    }
    return sum;
}

double ArrayAvg(double numbers[], int size) {
    return ArraySum(numbers, size) / size;
}
