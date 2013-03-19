%A = importdata('/home/ahmed/Desktop/ICDM13/datasets/nips_matrix/0.txt', ',');
k = 10;
l = 12;
i = 2;
numRows = 1500;
%[U1, S1, V1] = HalkoSVD(X, k, l, i);
[U2,S2,V2,E] = LrgSclHalkoSVD('/home/ahmed/Desktop/ICDM13/datasets/nips_matrix_mat/', k, l, i, numRows);
%error = norm(A-U*S*V')
