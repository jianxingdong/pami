function [ S,E ] = LrgSclGreedySelection(matrixDir, k,l )
%S is the set of the selected columns
%E is the approximation error
%l select k+l from each part

partsList = dir(matrixDir);
numParts = size(partsList,1); %will have to skip . and ..
H = []; %select columns
for i = 3: numParts
    load(strcat(matrixDir,partsList(i).name),'A');
    %the function selects subset of rows, so we transpose the input matrix to
    %select subset of columns
    [S, W]= GreedySelection(A', A', k+l);
    H = [H A(:,S)];
end
%Apply GCSS to H
[S, W]= GreedySelection(H', H', k);
C = H(:,S);

%calc E
E = 0;
for i = 3: numParts
    load(strcat(matrixDir,partsList(i).name),'A');
    E = E + Error(A,C); %make sure Error works fine with columns selection
end

end
