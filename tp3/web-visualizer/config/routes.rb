WebVisualizer::Application.routes.draw do

  root 'charts#index'

  get 'tweets' => 'charts#tweets_for', as: 'tweets_for'
end
