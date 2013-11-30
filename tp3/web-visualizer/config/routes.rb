WebVisualizer::Application.routes.draw do

  root 'charts#index'

  get 'tweets' => 'charts#tweets_for', as: 'tweets_for'

  get 'miles' => 'charts#miles', as: 'miles'
end
