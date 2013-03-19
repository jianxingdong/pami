%[S,E] = RandomSelection('/home/ahmed/Desktop/ICDM13/datasets/nips_matrix/', 100, 12419);
function [ S,E ] = RandomSelection(matrixDir, k, numCols )
S = randperm(numCols,k);
partsList = dir(matrixDir);
numParts = size(partsList,1); %will have to skip . and ..

from = 1;
to = 0;
C = [];
for i = 3: numParts
    load(strcat(matrixDir,partsList(i).name),'A');
    n = size(A,2);
    to = to + n;
    r = S(S(1,:) >= from & S(1,:) <= to);
    r = r -from + 1;
    C = [C A(:,r)];
    from = from + n;
end
%calc E
E = 0;
for i = 3: numParts
    load(strcat(matrixDir,partsList(i).name),'A');
    E = E + Error(A,C); %make sure Error works fine with columns selection
end

end
