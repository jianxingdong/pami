m = 47236;
n = 193844;
k = 100;


%random
%S = randperm(n,k);
%err = Error(feature,feature(:,S));

%svds 
%[U, S, V]= svds(feature,k);
%US = U*S;

%ssvd
%i = 0;  %0, 1, or 2
%p = 10;  %oversampling: should not exceed 10% of k
%[U, S, V]= HalkoSVD(feature, k, k+p, i);
%US = U*S;

% normsSum = 0;
% p = 50;
% numParts = ceil(n/p);
% from = 1;
% for j = 1:p
%     display(j)
%     to = from + numParts;
%     if(j==p)
%         to = n;
%     end
%     r = US*(V(from:to,:))';
%     normsSum = normsSum + norm(feature(:,from:to)-r, 'fro')^2;
%     clear r;
%     from = from + numParts;
% end
%err = sqrt(normsSum);

%Greedy
numGroups = 100;
target = randGroup(feature,numGroups);

%MR Greedy
% numParts = 40;
% incRatio = 0.2;
% [S,W ] = MR_GCSS_Stub(target, feature,k,numParts, incRatio);
% err = Error(feature,feature(:,S))

%Exact Greedy
[S]= GreedySelection(target, feature, k);
clear target
errEx = Error(feature,feature(:,S));
